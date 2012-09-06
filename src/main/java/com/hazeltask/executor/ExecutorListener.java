package com.hazeltask.executor;

import com.succinctllc.hazelcast.work.HazelcastWork;

public interface ExecutorListener {
    /**
     * Return false to cancel execution
     * @param runnable
     * @return
     */
    public boolean beforeExecute(HazelcastWork runnable);
    public void afterExecute(HazelcastWork runnable, Throwable exception);
}