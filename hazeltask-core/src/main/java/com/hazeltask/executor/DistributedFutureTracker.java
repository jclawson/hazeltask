package com.hazeltask.executor;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.executor.metrics.ExecutorMetrics;
import com.hazeltask.executor.metrics.LocalFuturesWaitingGauge;
import com.hazeltask.executor.task.HazeltaskTask;
import com.hazeltask.executor.task.TaskResponse;
import com.hazeltask.executor.task.TaskResponse.Status;
import com.yammer.metrics.core.Histogram;

public class DistributedFutureTracker<GROUP extends Serializable> implements MessageListener<TaskResponse<Serializable>> {
    private static ILogger LOGGER = Logger.getLogger(DistributedFutureTracker.class.getName());
    
    
    private Cache<UUID, DistributedFuture<Serializable>> futures;
    
    private final Histogram futureWaitTimeHistogram;
    
    /**
     * 
     * @param metrics (nullable)
     */
    public DistributedFutureTracker(ExecutorMetrics metrics, ExecutorConfig<GROUP> config) {
        futures = CacheBuilder.newBuilder()
                //no future will wait for more than this time
                .expireAfterAccess(config.getMaximumFutureWaitTime(), TimeUnit.MILLISECONDS)
                .weakValues() //if you don't reference your future anymore... we won't either
                .removalListener(new RemovalListener<UUID, DistributedFuture<Serializable>>() {
                    @Override
                    public void onRemoval(RemovalNotification<UUID, DistributedFuture<Serializable>> notification) {
                        if(notification.getCause() == RemovalCause.EXPIRED) {
                            long waitTimeMillis = System.currentTimeMillis() - notification.getValue().getCreatedTime();
                            notification.getValue().set(new TimeoutException("Future timed out waiting.  Waited "+(TimeUnit.MILLISECONDS.toMinutes(waitTimeMillis))+" minutes"));
                        } else if(notification.getCause() == RemovalCause.COLLECTED) {
                            //future was GC'd because we didn't want to track it
                            LOGGER.log(Level.FINEST, "Future "+notification.getKey()+" was garabge collected and removed from the tracker");
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
    public <T> DistributedFuture<T> createFuture(HazeltaskTask<GROUP> task) {
        DistributedFuture<T> future = new DistributedFuture<T>();
        this.futures.put(task.getId(), (DistributedFuture<Serializable>) future);
        return future;
    }
    
    protected DistributedFuture<Serializable> remove(UUID id) {
        DistributedFuture<Serializable> f =futures.getIfPresent(id); 
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
        DistributedFuture<Serializable> future = remove(taskId);
        if(future != null) {
            if(response.getStatus() == Status.FAILURE) {
                future.set(response.getError());
            } else if(response.getStatus() == Status.SUCCESS) {
                future.set((Serializable)response.getResponse());
            } else if (response.getStatus() == Status.CANCELLED) {
                future.setCancelled();
            }
        }
    }

    /**
     * handles when a member leaves and hazelcast partition data is lost.  We want 
     * to find the Futures that are waiting on lost data and error them
     */
    public void errorFuture(UUID taskId, Exception e) {
        DistributedFuture<Serializable> future = remove(taskId);
        if(future != null) {
            future.set(e);
        }
    }
    
    public int size() {
    	return (int) futures.size();
    }
    
    
}
