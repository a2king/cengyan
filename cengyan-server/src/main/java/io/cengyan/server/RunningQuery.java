package io.cengyan.server;

/** 某一刻正在执行的语句。 */
record RunningQuery(String queryId, String user, long elapsedMs, String statement) {
}
