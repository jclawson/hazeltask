package com.hazeltask.executor;

import java.io.Serializable;
import java.util.logging.Level;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazeltask.executor.task.HazeltaskTask;

public class ResponseExecutorListener<ID extends Serializable, G extends Serializable> implements ExecutorListener<ID,G> {
    
    private IExecutorTopologyService<ID, G> service;
    private ILogger LOGGER;
    
    public ResponseExecutorListener(IExecutorTopologyService<ID, G> service, LoggingService loggingService) {
        this.service = service;
        LOGGER = loggingService.getLogger(ResponseExecutorListener.class.getName());
    }
    
    public void afterExecute(HazeltaskTask<ID,G> runnable, Throwable exception) {
        //we finished this work... lets tell everyone about it!
        HazeltaskTask<ID,G> task = (HazeltaskTask<ID,G>)runnable;
        boolean success = exception == null && task.getException() == null;
        
        
        
        try {
            //Member me = topology.getHazelcast().getCluster().getLocalMember();
            if(success) {
                service.broadcastTaskCompletion(task.getId(), (Serializable)task.getResult());
                //response = new WorkResponse(me, work.getUniqueIdentifier(), (Serializable)work.getResult(), WorkResponse.Status.SUCCESS);
            } else {
                Throwable resolvedException = (task.getException() != null) ? task.getException() : exception;
                service.broadcastTaskError(task.getId(), resolvedException);
                //response = new WorkResponse(me, work.getUniqueIdentifier(), work.getException());
            }
            //TODO: handle work cancellation
            
            //topology.getWorkResponseTopic().publish(response);
            //service.broadcastTaskCompletion(response);
        } catch(RuntimeException e) {
            LOGGER.log(Level.SEVERE, "An error occurred while attempting to notify members of completed task", e);
        }
        
        
    }

    public void beforeExecute(HazeltaskTask<ID,G> runnable) {}
}
