package com.hazeltask.executor.local;

import java.io.Serializable;
import java.util.logging.Level;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazeltask.executor.ExecutorListener;
import com.hazeltask.executor.IExecutorTopologyService;
import com.hazeltask.executor.task.HazeltaskTask;

public class ResponseExecutorListener< G extends Serializable> implements ExecutorListener<G> {
    
    private IExecutorTopologyService<G> service;
    private static ILogger LOGGER = Logger.getLogger(ResponseExecutorListener.class.getName());
    
    public ResponseExecutorListener(IExecutorTopologyService<G> service) {
        this.service = service;
    }
    
    public void afterExecute(HazeltaskTask<G> runnable, Throwable exception) {
        //we finished this work... lets tell everyone about it!
        HazeltaskTask<G> task = (HazeltaskTask<G>)runnable;
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

    public void beforeExecute(HazeltaskTask<G> runnable) {}
}
