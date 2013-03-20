package com.hazeltask.executor;

import java.io.Serializable;

import com.hazeltask.executor.task.HazeltaskTask;


public interface ExecutorListener< G extends Serializable>{
    public void beforeExecute(HazeltaskTask<G> runnable);
    public void afterExecute(HazeltaskTask<G> runnable, Throwable exception);
}