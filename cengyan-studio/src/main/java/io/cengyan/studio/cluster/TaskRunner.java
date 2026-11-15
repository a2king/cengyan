package io.cengyan.studio.cluster;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;

/**
 * 集群任务：同一时间只运行一个。每一步失败后任务暂停，可重试本步、跳过本步或终止。
 * 进度写入 {@code data/tasks/<id>.json}；Studio 重启后运行中的任务显示为"已中断"，可继续。
 * 步骤必须可重复执行：继续或重试时会从当前步骤重新开始。
 */
public final class TaskRunner implements AutoCloseable {
    public enum State { RUNNING, PAUSED, INTERRUPTED, DONE, ABORTED }
    public enum StepState { PENDING, RUNNING, DONE, FAILED, SKIPPED }

    public record StepView(String title, StepState state, long startedAt, long endedAt, List<String> log) { }
    public record TaskView(String id, String type, String title, Map<String, String> params, State state, int current,
                           String error, long createdAt, long updatedAt, List<StepView> steps) {
        public boolean active() { return state == State.RUNNING || state == State.PAUSED || state == State.INTERRUPTED; }
    }

    public interface Context {
        void log(String message);
        String param(String key);
        /** 持久化到任务参数，Studio 重启后继续时可读回。 */
        void remember(String key, String value);
        void checkCanceled();
        /** 每 2 秒检查一次，超时抛出带说明的异常。 */
        void await(String what, Duration timeout, BooleanSupplier done);
    }

    public interface Action { void run(Context context) throws Exception; }
    public record Step(String title, Action action) { }
    public record Plan(String title, List<Step> steps, Action onAbort) { }

    /** 根据任务类型和参数生成步骤；继续中断的任务时也用它重建步骤。 */
    public interface Planner { Plan plan(String type, Map<String, String> params); }

    private static final int MAX_LOG_LINES = 400;
    private final Path dir;
    private final Planner planner;
    private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r, "studio-cluster-task"); thread.setDaemon(true); return thread;
    });
    private final Map<String, TaskView> tasks = new LinkedHashMap<>();
    private Plan plan;
    private volatile boolean cancel;

    public TaskRunner(Path dataDir, Planner planner) throws IOException {
        this.dir = dataDir.resolve("tasks");
        this.planner = planner;
        Files.createDirectories(dir);
        try (var files = Files.list(dir)) {
            for (Path file : files.filter(f -> f.toString().endsWith(".json")).toList()) {
                try {
                    TaskView task = ClusterStore.JSON.readValue(file.toFile(), TaskView.class);
                    if (task.state() == State.RUNNING) task = copy(task, State.INTERRUPTED, task.current(), "Studio 重启，任务已中断，可继续执行", task.steps(), task.params());
                    tasks.put(task.id(), task);
                    persist(task);
                } catch (IOException ignored) { }
            }
        }
    }

    /** await 中抛出时立即失败，不再等待（例如分片已丢失）。 */
    public static final class Fail extends RuntimeException {
        public Fail(String message) { super(message); }
    }

    private static final class Canceled extends RuntimeException {
        Canceled() { super("任务已终止"); }
    }

    public synchronized TaskView active() {
        return tasks.values().stream().filter(TaskView::active).findFirst().orElse(null);
    }

    public synchronized List<TaskView> list(int limit) {
        List<TaskView> result = new ArrayList<>(tasks.values());
        result.sort(Comparator.comparingLong(TaskView::createdAt).reversed());
        return result.subList(0, Math.min(limit, result.size()));
    }

    public synchronized TaskView get(String id) {
        TaskView task = tasks.get(id);
        if (task == null) throw new ClusterError("NOT_FOUND", "任务不存在：" + id);
        return task;
    }

    public synchronized TaskView submit(String type, Map<String, String> params) {
        TaskView running = active();
        if (running != null) throw new ClusterError("BUSY", "已有任务进行中：" + running.title() + "。请等待完成或在任务页处理");
        Plan next = planner.plan(type, params);
        long now = System.currentTimeMillis();
        String id = String.format("%tY%<tm%<td-%<tH%<tM%<tS-", now) + type.replace('.', '-') + "-" + UUID.randomUUID().toString().substring(0, 4);
        List<StepView> steps = next.steps().stream().map(s -> new StepView(s.title(), StepState.PENDING, 0, 0, List.of())).toList();
        TaskView task = new TaskView(id, type, next.title(), new LinkedHashMap<>(params), State.RUNNING, 0, "", now, now, steps);
        save(task);
        plan = next;
        cancel = false;
        worker.execute(() -> runFrom(id));
        return task;
    }

    /** action: retry（从当前步骤继续）、skip（跳过当前步骤后继续）、abort（终止，执行清理）。 */
    public synchronized TaskView act(String id, String action, Map<String, String> extra) {
        TaskView task = get(id);
        if (!task.active()) throw new ClusterError("INVALID_STATE", "任务已结束");
        Map<String, String> params = new LinkedHashMap<>(task.params());
        if (extra != null) params.putAll(extra);
        switch (action) {
            case "abort" -> {
                if (task.state() == State.RUNNING) {
                    save(copy(task, task.state(), task.current(), task.error(), task.steps(), params));
                    cancel = true;
                    return tasks.get(id);
                }
                // Cleanup may call the cluster; show the task as busy until it finishes.
                save(copy(task, State.RUNNING, task.current(), "正在终止", task.steps(), params));
                if (plan == null) plan = planner.plan(task.type(), params);
                worker.execute(() -> abort(id));
                return tasks.get(id);
            }
            case "retry", "skip" -> {
                if (task.state() == State.RUNNING) throw new ClusterError("INVALID_STATE", "任务正在运行");
                List<StepView> steps = new ArrayList<>(task.steps());
                int current = task.current();
                if (action.equals("skip") && current < steps.size()) {
                    StepView step = steps.get(current);
                    steps.set(current, new StepView(step.title(), StepState.SKIPPED, step.startedAt(), System.currentTimeMillis(), append(step.log(), "已手动跳过")));
                    current++;
                }
                plan = planner.plan(task.type(), params);
                cancel = false;
                save(copy(task, State.RUNNING, current, "", steps, params));
                worker.execute(() -> runFrom(id));
                return tasks.get(id);
            }
            default -> throw new ClusterError("INVALID_ARGUMENT", "未知操作：" + action);
        }
    }

    private void runFrom(String id) {
        while (true) {
            Step step;
            int index;
            boolean cancelPending = false;
            synchronized (this) {
                TaskView task = tasks.get(id);
                if (task.state() != State.RUNNING) return;
                if (cancel) { cancelPending = true; }
                index = task.current();
                if (!cancelPending && index >= plan.steps().size()) {
                    save(copy(task, State.DONE, index, "", task.steps(), task.params()));
                    return;
                }
                if (!cancelPending) {
                    step = plan.steps().get(index);
                    setStep(id, index, StepState.RUNNING, "开始");
                } else step = null;
            }
            if (step == null) { abort(id); return; }
            try {
                step.action().run(context(id, index));
                synchronized (this) {
                    setStep(id, index, StepState.DONE, "完成");
                    TaskView task = tasks.get(id);
                    save(copy(task, State.RUNNING, index + 1, "", task.steps(), task.params()));
                }
            } catch (Canceled ex) {
                synchronized (this) { setStep(id, index, StepState.FAILED, "已终止"); }
                abort(id);
                return;
            } catch (Throwable ex) {
                String message = message(ex);
                synchronized (this) {
                    setStep(id, index, StepState.FAILED, "失败：" + message);
                    TaskView task = tasks.get(id);
                    save(copy(task, cancel ? State.RUNNING : State.PAUSED, index, message, task.steps(), task.params()));
                }
                if (cancel) { abort(id); return; }
                return;
            }
        }
    }

    private void abort(String id) {
        try {
            if (plan != null && plan.onAbort() != null) plan.onAbort().run(context(id, -1));
        } catch (Throwable ex) {
            synchronized (this) { appendTaskError(id, "终止清理失败：" + message(ex)); }
        }
        synchronized (this) {
            TaskView task = tasks.get(id);
            String error = task.error().replace("正在终止", "").replaceFirst("^；", "");
            save(copy(task, State.ABORTED, task.current(), error.isEmpty() ? "已终止" : "已终止；" + error, task.steps(), task.params()));
            cancel = false;
        }
    }

    private Context context(String id, int index) {
        return new Context() {
            private long lastProgress;
            @Override public void log(String message) {
                synchronized (TaskRunner.this) {
                    if (index < 0) { appendTaskError(id, message); return; }
                    TaskView task = tasks.get(id);
                    StepView step = task.steps().get(index);
                    replace(id, index, new StepView(step.title(), step.state(), step.startedAt(), step.endedAt(), append(step.log(), message)));
                }
            }
            @Override public String param(String key) {
                synchronized (TaskRunner.this) { return tasks.get(id).params().getOrDefault(key, ""); }
            }
            @Override public void remember(String key, String value) {
                synchronized (TaskRunner.this) {
                    TaskView task = tasks.get(id);
                    Map<String, String> params = new LinkedHashMap<>(task.params());
                    params.put(key, value);
                    save(copy(task, task.state(), task.current(), task.error(), task.steps(), params));
                }
            }
            @Override public void checkCanceled() { if (cancel) throw new Canceled(); }
            @Override public void await(String what, Duration timeout, BooleanSupplier done) {
                long deadline = System.nanoTime() + timeout.toNanos();
                String last = "";
                while (true) {
                    checkCanceled();
                    try { if (done.getAsBoolean()) return; last = ""; }
                    catch (Fail | Canceled ex) { throw ex; }
                    catch (RuntimeException ex) { last = message(ex); }
                    if (System.nanoTime() > deadline)
                        throw new ClusterError("TIMEOUT", "等待" + what + "超时（" + timeout.toMinutes() + " 分钟）" + (last.isEmpty() ? "" : "，最后一次检查：" + last));
                    if (System.currentTimeMillis() - lastProgress > 30_000) {
                        lastProgress = System.currentTimeMillis();
                        log("等待" + what + (last.isEmpty() ? "" : "（" + last + "）"));
                    }
                    try { Thread.sleep(2000); }
                    catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new Canceled(); }
                }
            }
        };
    }

    private void setStep(String id, int index, StepState state, String message) {
        TaskView task = tasks.get(id);
        StepView step = task.steps().get(index);
        long now = System.currentTimeMillis();
        long started = state == StepState.RUNNING ? now : step.startedAt();
        long ended = state == StepState.RUNNING ? 0 : now;
        replace(id, index, new StepView(step.title(), state, started, ended, append(step.log(), message)));
    }

    private void replace(String id, int index, StepView step) {
        TaskView task = tasks.get(id);
        List<StepView> steps = new ArrayList<>(task.steps());
        steps.set(index, step);
        save(copy(task, task.state(), task.current(), task.error(), steps, task.params()));
    }

    private void appendTaskError(String id, String message) {
        TaskView task = tasks.get(id);
        String error = task.error().isEmpty() ? message : task.error() + "；" + message;
        save(copy(task, task.state(), task.current(), error, task.steps(), task.params()));
    }

    private static TaskView copy(TaskView task, State state, int current, String error, List<StepView> steps, Map<String, String> params) {
        return new TaskView(task.id(), task.type(), task.title(), Map.copyOf(params), state, current, error == null ? "" : error,
                task.createdAt(), System.currentTimeMillis(), List.copyOf(steps));
    }

    private void save(TaskView task) {
        tasks.put(task.id(), task);
        persist(task);
        if (tasks.size() > 100) {
            tasks.values().stream().filter(t -> !t.active()).min(Comparator.comparingLong(TaskView::createdAt)).ifPresent(old -> {
                tasks.remove(old.id());
                try { Files.deleteIfExists(dir.resolve(old.id() + ".json")); } catch (IOException ignored) { }
            });
        }
    }

    private void persist(TaskView task) {
        try { ClusterStore.write(dir.resolve(task.id() + ".json"), task); }
        catch (IOException ex) { java.util.logging.Logger.getLogger(TaskRunner.class.getName()).warning("任务进度保存失败：" + ex); }
    }

    private static List<String> append(List<String> log, String message) {
        List<String> next = new ArrayList<>(log);
        next.add(String.format("%tT %s", System.currentTimeMillis(), message));
        while (next.size() > MAX_LOG_LINES) next.remove(0);
        return List.copyOf(next);
    }

    static String message(Throwable ex) {
        Throwable cause = ex;
        while (cause instanceof java.lang.reflect.InvocationTargetException || cause instanceof java.util.concurrent.ExecutionException) cause = cause.getCause();
        String text = cause.getMessage();
        return text == null || text.isBlank() ? cause.getClass().getSimpleName() : text;
    }

    @Override public void close() { cancel = true; worker.shutdownNow(); }
}
