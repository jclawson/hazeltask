package com.succinctllc.hazelcast.work.executor;

import java.util.Collection;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.succinctllc.hazelcast.work.WorkResponse;
import com.succinctllc.hazelcast.work.WorkResponse.Status;


public class DistributedFutureTracker implements MessageListener<WorkResponse> {
    private DistributedExecutorService service;
    
    private SetMultimap<String, DistributedFuture<?>> futures = 
            Multimaps.<String, DistributedFuture<?>>synchronizedSetMultimap(
                HashMultimap.<String, DistributedFuture<?>>create()
            );
    
    public DistributedFutureTracker(DistributedExecutorService service) {
        this.service = service;
        
        ITopic<WorkResponse> topic = this.service.getTopology().getWorkResponseTopic();
        topic.addMessageListener(this);
    }
    
    public void add(String id, DistributedFuture<?> future) {
        this.futures.put(id, future);
    }
    
    public void onMessage(Message<WorkResponse> message) {
        WorkResponse response = message.getMessageObject();
        String workId = response.getWorkId();
        Collection<DistributedFuture<?>> workFutures = futures.removeAll(workId);
        if(workFutures.size() > 0) {
            for(DistributedFuture future : workFutures) {
                if(response.getStatus() == Status.FAILURE) {
                    future.set(response.getError());
                } else if(response.getStatus() == Status.SUCCESS) {
                    future.set(response.getResponse());
                } else if (response.getStatus() == Status.CANCELLED) {
                    future.setCancelled();
                }
            }
        }
    }
    
    public int size() {
    	return futures.size();
    }
    
    
}
