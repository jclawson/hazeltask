package com.succinctllc.hazelcast.work.executor;

public interface ExecutorListener {
    public void afterExecute(Runnable runnable, Throwable exception);
    public void beforeExecute(Runnable runnable);
}