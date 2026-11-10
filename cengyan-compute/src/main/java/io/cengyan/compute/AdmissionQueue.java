package io.cengyan.compute;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/**
 * 入口有界队列。调度者周期调用 poll/sweep，取得 Ticket 后才向远端预留执行名额。
 * 所有转换串行化，超时/取消后不能开始；执行名额与入口队列相互独立。
 */
public final class AdmissionQueue<T> {
    public enum State { QUEUED, RESERVING, RUNNING, SUCCEEDED, FAILED, CANCELED, TIMED_OUT }
    public record Outcome(String code, String message) { }

    public static final class Ticket<T> {
        private final String requestId;
        private final T request;
        private final long queueDeadline;
        private final long hardDeadline;
        private final CompletableFuture<Outcome> completion = new CompletableFuture<>();
        private volatile State state = State.QUEUED;

        private Ticket(String requestId, T request, long queueDeadline, long hardDeadline) {
            this.requestId = requestId;
            this.request = request;
            this.queueDeadline = queueDeadline;
            this.hardDeadline = hardDeadline;
        }
        public String requestId() { return requestId; }
        public T request() { return request; }
        public State state() { return state; }
        public CompletableFuture<Outcome> completion() { return completion.copy(); }
    }

    private final int capacity;
    private final long queueTimeoutNanos;
    private final LongSupplier clock;
    private final ArrayDeque<Ticket<T>> waiting = new ArrayDeque<>();
    private final Map<String, Ticket<T>> tracked = new HashMap<>();
    private final java.util.Set<Ticket<T>> executing = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private boolean draining;

    public AdmissionQueue(int capacity, Duration queueTimeout) { this(capacity, queueTimeout, System::nanoTime); }
    AdmissionQueue(int capacity, Duration queueTimeout, LongSupplier clock) {
        if (capacity < 1 || queueTimeout == null || queueTimeout.isNegative() || queueTimeout.isZero()
                || queueTimeout.compareTo(Duration.ofHours(24)) > 0) throw new IllegalArgumentException("入口队列配置无效");
        this.capacity = capacity;
        this.queueTimeoutNanos = queueTimeout.toNanos();
        this.clock = Objects.requireNonNull(clock);
    }

    public synchronized Ticket<T> submit(String requestId, T request, Duration remainingTime) {
        if (requestId == null || requestId.isBlank() || request == null || remainingTime == null
                || remainingTime.isNegative() || remainingTime.isZero() || remainingTime.compareTo(Duration.ofHours(24)) > 0) {
            throw new IllegalArgumentException("请求和截止时间无效");
        }
        sweep();
        if (tracked.containsKey(requestId) || executing.stream().anyMatch(ticket -> ticket.requestId.equals(requestId))) {
            throw new IllegalArgumentException("入口已接纳相同请求或仍在清理该请求");
        }
        long now = clock.getAsLong(), budget = remainingTime.toNanos();
        Ticket<T> ticket = new Ticket<>(requestId, request, now + Math.min(queueTimeoutNanos, budget), now + budget);
        if (draining || queuedCount() >= capacity) {
            ticket.state = State.FAILED;
            ticket.completion.complete(new Outcome("BUSY", draining ? "入口正在排空" : "入口队列已满"));
        } else {
            tracked.put(requestId, ticket);
            waiting.addLast(ticket);
        }
        return ticket;
    }

    public synchronized Ticket<T> poll() {
        sweep();
        Ticket<T> ticket = waiting.pollFirst();
        if (ticket != null) ticket.state = State.RESERVING;
        return ticket;
    }

    /** 无候选或远端拒绝预留时重新排队，不延长排队 deadline。 */
    public synchronized boolean retryReservation(Ticket<T> ticket) {
        if (!owns(ticket) || ticket.state != State.RESERVING) return false;
        if (expire(ticket)) return false;
        ticket.state = State.QUEUED;
        waiting.addLast(ticket);
        return true;
    }

    /** 必须在真正转发执行前调用；false 时调用方释放已经取得的远端预留。 */
    public synchronized boolean start(Ticket<T> ticket) {
        if (!owns(ticket) || ticket.state != State.RESERVING || expire(ticket)) return false;
        ticket.state = State.RUNNING;
        executing.add(ticket);
        return true;
    }

    public synchronized boolean finish(Ticket<T> ticket, Outcome outcome) {
        Objects.requireNonNull(outcome);
        if (!owns(ticket) || ticket.state != State.RUNNING) return false;
        executing.remove(ticket);
        terminal(ticket, "OK".equals(outcome.code) ? State.SUCCEEDED : State.FAILED, outcome);
        return true;
    }

    /** 返回 true 后调用方传播 CANCEL；已持久化写事务仍由 Storage 负责收尾。 */
    public synchronized boolean cancel(Ticket<T> ticket) {
        if (!owns(ticket)) return false;
        terminal(ticket, State.CANCELED, new Outcome("QUERY_CANCELED", "客户端已断开或请求被取消"));
        return true;
    }

    /** 超时/取消响应已经结束，但排空仍需等待远端执行器确认资源清理完成。 */
    public synchronized boolean cleanupComplete(Ticket<T> ticket) {
        if (ticket == null || ticket.state == State.RUNNING) return false;
        return executing.remove(ticket);
    }

    public synchronized void sweep() {
        for (Ticket<T> ticket : java.util.List.copyOf(tracked.values())) expire(ticket);
    }

    private boolean expire(Ticket<T> ticket) {
        long now = clock.getAsLong();
        boolean running = ticket.state == State.RUNNING;
        if (now - (running ? ticket.hardDeadline : ticket.queueDeadline) < 0) return false;
        terminal(ticket, State.TIMED_OUT, new Outcome(running ? "QUERY_TIMEOUT" : "QUEUE_TIMEOUT",
                running ? "请求已到达执行截止时间" : "入口排队超时，尚未开始执行"));
        return true;
    }

    private void terminal(Ticket<T> ticket, State state, Outcome outcome) {
        waiting.remove(ticket);
        tracked.remove(ticket.requestId, ticket);
        ticket.state = state;
        ticket.completion.complete(outcome);
    }
    private boolean owns(Ticket<T> ticket) { return ticket != null && tracked.get(ticket.requestId) == ticket; }
    private int queuedCount() { return (int) tracked.values().stream().filter(t -> t.state != State.RUNNING).count(); }
    public synchronized int pending() { sweep(); return queuedCount(); }
    public synchronized void beginDrain() { draining = true; }
    public synchronized boolean drained() { sweep(); return draining && tracked.isEmpty() && executing.isEmpty(); }
}
