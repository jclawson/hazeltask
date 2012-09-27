package com.hazeltask.executor;

import com.hazeltask.executor.task.HazeltaskTask;


public interface ExecutorListener {
    /**
     * Return false to cancel execution
     * @param runnable
     * @return
     */
    public boolean beforeExecute(HazeltaskTask runnable);
    public void afterExecute(HazeltaskTask runnable, Throwable exception);
}