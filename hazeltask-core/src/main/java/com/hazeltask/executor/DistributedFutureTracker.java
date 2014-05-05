package com.hazeltask.executor;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Histogram;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.executor.metrics.ExecutorMetrics;
import com.hazeltask.executor.metrics.LocalFuturesWaitingGauge;
import com.hazeltask.executor.task.HazeltaskTask;
import com.hazeltask.executor.task.TaskResponse;
import com.hazeltask.executor.task.TaskResponse.Status;

@Slf4j
public class DistributedFutureTracker<GROUP extends Serializable> implements MessageListener<TaskResponse<Serializable>> {
    private Cache<UUID, DistributedFuture<GROUP, Serializable>> futures;
    
    private final Histogram futureWaitTimeHistogram;
    private final IExecutorTopologyService<GROUP> topologyService;
    
    /**
     * 
     * @param metrics (nullable)
     */
    public DistributedFutureTracker(final IExecutorTopologyService<GROUP> topologyService, ExecutorMetrics metrics, ExecutorConfig<GROUP> config) {
        this.topologyService = topologyService;
        futures = CacheBuilder.newBuilder()
                //no future will wait for more than this time
                .expireAfterAccess(config.getMaximumFutureWaitTime(), TimeUnit.MILLISECONDS)
                .removalListener(new RemovalListener<UUID, DistributedFuture<GROUP, Serializable>>() {
                    @Override
                    public void onRemoval(RemovalNotification<UUID, DistributedFuture<GROUP, Serializable>> notification) {
                        if(notification.getCause() == RemovalCause.EXPIRED) {
                        	DistributedFuture<GROUP, Serializable> future = notification.getValue();
                            long waitTimeMillis = System.currentTimeMillis() - future.getCreatedTime();
                            notification.getValue().setException(new TimeoutException("Future timed out waiting.  Waited "+(TimeUnit.MILLISECONDS.toMinutes(waitTimeMillis))+" minutes"));
                            
                            topologyService.cancelTask(future.getGroup(), future.getTaskId()); 
                            topologyService.removePendingTask(future.getTaskId());
                        } else if(notification.getCause() == RemovalCause.COLLECTED) {
                            //future was GC'd because we didn't want to track it
                            log.debug("Future "+notification.getKey()+" was garabge collected and removed from the tracker");
                        }
                    }
                 })
                .build();
        
        if(metrics != null) {
            metrics.registerLocalFuturesWaitingGauge(new LocalFuturesWaitingGauge(this));
            futureWaitTimeHistogram = metrics.getFutureWaitTimeHistogram().getMetric();
        } else {
            futureWaitTimeHistogram = null;
        }
    }
    
    //It is required that T be Serializable
    @SuppressWarnings("unchecked")
    public <T> DistributedFuture<GROUP, T> createFuture(HazeltaskTask<GROUP> task) {
        DistributedFuture<GROUP, T> future = new DistributedFuture<GROUP, T>(topologyService, task.getGroup(), task.getId());
        this.futures.put(task.getId(), (DistributedFuture<GROUP, Serializable>) future);
        return future;
    }
    
    protected DistributedFuture<GROUP, Serializable> remove(UUID id) {
        DistributedFuture<GROUP, Serializable> f =futures.getIfPresent(id); 
        futures.invalidate(id);
        
       if(f != null) {
         if(futureWaitTimeHistogram != null) {
            futureWaitTimeHistogram.update(System.currentTimeMillis() - f.getCreatedTime());
         }
       }
         
         return f;
    }
    
    public Set<UUID> getTrackedTaskIds() {
        return new HashSet<UUID>(futures.asMap().keySet());
    }
    
    @Override
    public void onMessage(Message<TaskResponse<Serializable>> message) {
        TaskResponse<Serializable> response = message.getMessageObject();
        UUID taskId = response.getTaskId();
        DistributedFuture<GROUP, Serializable> future = remove(taskId);
        if(future != null) {
            if(response.getStatus() == Status.FAILURE) {
                future.setException(response.getError());
            } else if(response.getStatus() == Status.SUCCESS) {
                future.set((Serializable)response.getResponse());
            } else if (response.getStatus() == Status.CANCELLED) {
                //TODO: add a status for INTERRUPTED
                future.setCancelled(false);
            }
            
        }
    }

    /**
     * handles when a member leaves and hazelcast partition data is lost.  We want 
     * to find the Futures that are waiting on lost data and error them
     */
    public void errorFuture(UUID taskId, Exception e) {
        DistributedFuture<GROUP, Serializable> future = remove(taskId);
        if(future != null) {
            future.setException(e);
        }
    }
    
    public int size() {
    	return (int) futures.size();
    }
    
    
}
