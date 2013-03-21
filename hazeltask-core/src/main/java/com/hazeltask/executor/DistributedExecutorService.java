package com.hazeltask.executor;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.ListenableFuture;
import com.hazeltask.ServiceListenable;

public interface DistributedExecutorService<GROUP extends Serializable> extends ExecutorService, ServiceListenable<DistributedExecutorService<GROUP>> {
    public void startup();
    
    @Override
    public <T> ListenableFuture<T> submit(Callable<T> task);
    
    @Override
    public ListenableFuture<?> submit(Runnable task);
    
    @Override
    public <T> ListenableFuture<T> submit(Runnable task, T result);
}
