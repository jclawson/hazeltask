package com.hazeltask.executor;


public interface ExecutorListener {
    /**
     * Return false to cancel execution
     * @param runnable
     * @return
     */
    public boolean beforeExecute(HazelcastWork runnable);
    public void afterExecute(HazelcastWork runnable, Throwable exception);
}