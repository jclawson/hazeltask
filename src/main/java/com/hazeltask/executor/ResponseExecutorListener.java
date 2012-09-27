package com.hazeltask.executor;

import java.io.Serializable;
import java.util.logging.Level;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazeltask.executor.task.HazeltaskTask;

public class ResponseExecutorListener implements ExecutorListener {
    
    private IExecutorTopologyService service;
    private ILogger LOGGER;
    
    public ResponseExecutorListener(IExecutorTopologyService service, LoggingService loggingService) {
        this.service = service;
        LOGGER = loggingService.getLogger(ResponseExecutorListener.class.getName());
    }
    
    public void afterExecute(HazeltaskTask runnable, Throwable exception) {
        //we finished this work... lets tell everyone about it!
        HazeltaskTask task = (HazeltaskTask)runnable;
        boolean success = exception == null && task.getException() == null;
        
        
        
        try {
            //Member me = topology.getHazelcast().getCluster().getLocalMember();
            if(success) {
                service.broadcastTaskCompletion(task.getUniqueIdentifier(), (Serializable)task.getResult());
                //response = new WorkResponse(me, work.getUniqueIdentifier(), (Serializable)work.getResult(), WorkResponse.Status.SUCCESS);
            } else {
                Throwable resolvedException = (task.getException() != null) ? task.getException() : exception;
                service.broadcastTaskError(task.getUniqueIdentifier(), resolvedException);
                //response = new WorkResponse(me, work.getUniqueIdentifier(), work.getException());
            }
            //TODO: handle work cancellation
            
            //topology.getWorkResponseTopic().publish(response);
            //service.broadcastTaskCompletion(response);
        } catch(RuntimeException e) {
            LOGGER.log(Level.SEVERE, "An error occurred while attempting to notify members of completed task", e);
        }
        
        
    }

    public boolean beforeExecute(HazeltaskTask runnable) {return true;}
}
