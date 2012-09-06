package com.hazeltask.executor;

import java.io.Serializable;
import java.util.logging.Level;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazeltask.ITopologyService;
import com.succinctllc.hazelcast.work.HazelcastWork;

public class ResponseExecutorListener implements ExecutorListener {
    
    private ITopologyService service;
    private ILogger LOGGER;
    
    public ResponseExecutorListener(ITopologyService service, LoggingService loggingService) {
        this.service = service;
        LOGGER = loggingService.getLogger(ResponseExecutorListener.class.getName());
    }
    
    public void afterExecute(HazelcastWork runnable, Throwable exception) {
        //we finished this work... lets tell everyone about it!
        HazelcastWork work = (HazelcastWork)runnable;
        boolean success = exception == null && work.getException() == null;
        
        try {
            //Member me = topology.getHazelcast().getCluster().getLocalMember();
            if(success) {
                service.broadcastTaskCompletion(work.getUniqueIdentifier(), (Serializable)work.getResult());
                //response = new WorkResponse(me, work.getUniqueIdentifier(), (Serializable)work.getResult(), WorkResponse.Status.SUCCESS);
            } else {
                service.broadcastTaskError(work.getUniqueIdentifier(), work.getException());
                //response = new WorkResponse(me, work.getUniqueIdentifier(), work.getException());
            }
            //TODO: handle work cancellation
            
            //topology.getWorkResponseTopic().publish(response);
            //service.broadcastTaskCompletion(response);
        } catch(RuntimeException e) {
            LOGGER.log(Level.SEVERE, "An error occurred while attempting to notify members of completed work", e);
        }
        
        
    }

    public boolean beforeExecute(HazelcastWork runnable) {return true;}
}
