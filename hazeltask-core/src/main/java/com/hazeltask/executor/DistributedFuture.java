package com.hazeltask.executor;

import java.io.Serializable;
import java.util.UUID;

import com.google.common.util.concurrent.AbstractFuture;

/**
 * This future implements the ListenableFuture interface from Google Guava
 * 
 * @see https://code.google.com/p/guava-libraries/wiki/ListenableFutureExplained
 * @author jclawson
 *
 * @param <V>
 */
public class DistributedFuture<GROUP extends Serializable, V> extends AbstractFuture<V> {
    private final long createdTime;
    private final IExecutorTopologyService<GROUP> topologyService;
    private final GROUP group;
    private final UUID taskId;
    
    public DistributedFuture(IExecutorTopologyService<GROUP> topologyService, GROUP group, UUID taskId) {
        createdTime = System.currentTimeMillis();
        this.group = group;
        this.taskId = taskId;
        this.topologyService = topologyService;
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if(!this.isCancelled() && topologyService.cancelTask(group, taskId)) {
            return setCancelled(mayInterruptIfRunning);
        }
        return false;
    }
    
    public boolean setCancelled(boolean mayInterruptIfRunning) {
        return super.cancel(mayInterruptIfRunning);
    }
    
    /**
     * This is called by the superclass on successful cancel
     */
    protected void interruptTask() {}
    
    public boolean setException(Throwable e) {
        return super.setException(e);
    }
    
    public boolean set(V value) {
        return super.set(value);
    }
    
    public long getCreatedTime() {
        return this.createdTime;
    }  
}
