package com.hazeltask.executor;

import java.util.Arrays;
import java.util.Collection;



public class DelegatingExecutorListener implements ExecutorListener {
    public Collection<ExecutorListener> listeners; 
    public DelegatingExecutorListener(Collection<ExecutorListener> listeners) {
        this.listeners = listeners;
    }
    
    public DelegatingExecutorListener(ExecutorListener delegate) {
        this.listeners = Arrays.asList(delegate);
    }
    
    public void afterExecute(HazelcastWork runnable, Throwable exception) {
        for(ExecutorListener listener : listeners) {
            try {
                listener.afterExecute(runnable, exception);
            } catch(Throwable e) {
                //ignore
                //TODO: log
            }
        }
    }

    public boolean beforeExecute(HazelcastWork runnable) {
        boolean allow = true;
        for(ExecutorListener listener : listeners) {
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