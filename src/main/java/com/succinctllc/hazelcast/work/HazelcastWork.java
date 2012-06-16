package com.succinctllc.hazelcast.work;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.succinctllc.core.concurrent.collections.grouped.Groupable;

/**
 * This class wraps a runnable and provides other metadata we need to searching work items
 * in the distributed map.
 * 
 * FIXME: handling the completion tasks here is wrong... We need to move that logic out
 * so we can handle things like cancellation too!
 * 
 * @author jclawson
 *
 */
public class HazelcastWork implements Groupable, Runnable, Work {
	private static final long serialVersionUID = 1L;
	private static ILogger LOGGER = Logger.getLogger(HazelcastWork.class.getName());
	
	private Runnable task;
	private long createdAtMillis;
	private WorkId key;
	private String topology;
	private int submissionCount;
	
	public HazelcastWork(String topology, WorkId key, Runnable task){
		this.task = task;
		this.key = key;
		this.topology = topology;
		createdAtMillis = System.currentTimeMillis();
		this.submissionCount = 1;
	}
	
	public HazelcastWork(String topology, WorkId key, Callable<?> task){
        this.task = new CallableRunnable(task);
        this.key = key;
        this.topology = topology;
        createdAtMillis = System.currentTimeMillis();
        this.submissionCount = 1;
    }
	
	private static class CallableRunnable implements Runnable {
	    private Callable<?> task;
	    private Object result;
	    private Exception e;
	    
        public CallableRunnable(Callable<?> task) {
            this.task = task;
        }

        public void run() {
            try {
                result = task.call();
            } catch (Exception e) {
                this.e = e;
            }
        }

        public Object getResult() {
            return result;
        }

        public Exception getException() {
            return e;
        }
        
        public boolean isSuccess() {
            return e == null;
        }
	}
	
	public void setSubmissionCount(int submissionCount){
	    this.submissionCount = submissionCount;
	}
	
	public int getSubmissionCount(){
	    return this.submissionCount;
	}
	
	public void updateCreatedTime(){
	    this.createdAtMillis = System.currentTimeMillis();
	}
	
	public Runnable getDelegate(){
	    return task;
	}

	public String getGroup() {
		return key.getGroup();
	}

	public String getUniqueIdentifier() {
		return key.getId();
	}
	
	public long getTimeCreated(){
		return createdAtMillis;
	}

    public void run() {
		try {
		    task.run();
		} finally {
		    HazelcastWorkTopology topology = HazelcastWorkTopology.get(this.topology);
		    try {
    		    Member me = topology.getHazelcast().getCluster().getLocalMember();
    		    WorkResponse response;
    		    //alert futures of completion
    		    if(task instanceof CallableRunnable) {
    		        CallableRunnable callRun = (CallableRunnable)task;
    		        if(callRun.isSuccess()) {
    		            response = new WorkResponse(me, key.getId(), (Serializable)callRun.getResult(), WorkResponse.Status.SUCCESS);
    		        } else {
    		            response = new WorkResponse(me, key.getId(), "HazelcastWork error: "+callRun.getException());
    		        }
    		    } else {
    		        response = new WorkResponse(me, key.getId(), null, WorkResponse.Status.SUCCESS);
    		    }
    		    
    		    topology.getWorkResponseTopic().publish(response);
		    } catch(RuntimeException e) {
		        LOGGER.log(Level.SEVERE, "An error occurred while attempting to notify members of completed work", e);
		    }
		    //TODO: add task exceptions handling / retry logic
            //for now, just remove the work because its completed
		    topology.getPendingWork()
		        .remove(key.getId());
		}
	}

	public WorkId getWorkId() {
		return key;
	}

	
	
}
