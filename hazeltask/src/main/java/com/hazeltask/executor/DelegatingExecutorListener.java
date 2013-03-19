package com.hazeltask.executor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

import com.hazeltask.executor.task.HazeltaskTask;



public class DelegatingExecutorListener<ID extends Serializable,G extends Serializable> implements ExecutorListener<ID,G> {
    public Collection<ExecutorListener<ID,G>> listeners; 
    public DelegatingExecutorListener(Collection<ExecutorListener<ID,G>> listeners) {
        this.listeners = listeners;
    }
    
    @SuppressWarnings("unchecked")
    public DelegatingExecutorListener(ExecutorListener<ID,G> delegate) {
        this.listeners = (Collection<ExecutorListener<ID,G>>) Arrays.asList(delegate);
    }
    
    public void afterExecute(HazeltaskTask<ID,G> runnable, Throwable exception) {
        for(ExecutorListener<ID,G> listener : listeners) {
            try {
                listener.afterExecute(runnable, exception);
            } catch(Throwable e) {
                //ignore
                //TODO: log
            }
        }
    }

    public boolean beforeExecute(HazeltaskTask<ID,G> runnable) {
        boolean allow = true;
        for(ExecutorListener<ID,G> listener : listeners) {
            try {
                allow = listener.beforeExecute(runnable) && allow;
            } catch(Throwable e) {
                //ignore
                //TODO: log
            }
        }
        return allow;
    }
}