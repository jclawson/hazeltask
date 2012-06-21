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
	
	private Runnable runTask;
	private Callable<?> callTask;
	
	private long createdAtMillis;
	private WorkId key;
	private String topology;
	private int submissionCount;
	
	private volatile Object result;
    private volatile Exception e;
	
	public HazelcastWork(String topology, WorkId key, Runnable task){
		this.runTask = task;
		this.key = key;
		this.topology = topology;
		createdAtMillis = System.currentTimeMillis();
		this.submissionCount = 1;
	}
	
	public HazelcastWork(String topology, WorkId key, Callable<?> task){
        this.callTask = task;
        this.key = key;
        this.topology = topology;
        createdAtMillis = System.currentTimeMillis();
        this.submissionCount = 1;
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
	
//	public Runnable getDelegate(){
//	    return task;
//	}
	
	

	public String getGroup() {
		return key.getGroup();
	}

	public Object getResult() {
        return result;
    }

    public Exception getException() {
        return e;
    }

    public String getUniqueIdentifier() {
		return key.getId();
	}
	
	public long getTimeCreated(){
		return createdAtMillis;
	}

    public void run() {
        try {
            if(callTask != null) {
    		    this.result = callTask.call();
    		} else {
    		    runTask.run();
    		}
        } catch (Exception t) {
            this.e = t;
        }
	}

	public WorkId getWorkId() {
		return key;
	}

	
	
}
