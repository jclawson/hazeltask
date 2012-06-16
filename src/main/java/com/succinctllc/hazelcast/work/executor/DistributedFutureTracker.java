package com.succinctllc.hazelcast.work.executor;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.google.common.collect.Multimap;


public class DistributedFutureTracker {
    private DistributedExecutorService service;
    
    private Multimap<String, DistributedFutureTask<?>> futures;
    
    public DistributedFutureTracker(DistributedExecutorService service) {
        this.service = service;
    }
    
    public static class DistributedFutureTask<T> extends FutureTask<T> {
        public DistributedFutureTask(Runnable runnable, T result) {
            super(runnable, result);
        }
        
        public DistributedFutureTask(Callable<T> callable) {
            super(callable);
        }   
    }
    
    
}
