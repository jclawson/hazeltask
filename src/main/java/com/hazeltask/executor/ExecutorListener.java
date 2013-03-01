package com.hazeltask.executor;

import java.io.Serializable;

import com.hazeltask.executor.task.HazeltaskTask;


public interface ExecutorListener<ID extends Serializable, G extends Serializable>{
    /**
     * Return false to cancel execution
     * @param runnable
     * @return
     */
    public boolean beforeExecute(HazeltaskTask<ID,G> runnable);
    public void afterExecute(HazeltaskTask<ID,G> runnable, Throwable exception);
}