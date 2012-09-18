package com.hazeltask.executor;

import java.util.concurrent.Callable;

import com.hazeltask.core.concurrent.collections.grouped.Groupable;

/**
 * This class wraps a runnable and provides other metadata we need to searching work items
 * in the distributed map.
 * 
 * FIXME: handling the completion tasks here is wrong... We need to move that logic out
 * so we can handle things like cancellation too!
 * 
 * FIXME: make DataSerializable, use SerializationHelper for runTask / callTask... 
 *        write boolean to indicate if runnable / callable
 * 
 * @author jclawson
 *
 */
public class HazelcastWork implements Groupable, Runnable, Work {
	private static final long serialVersionUID = 1L;
	
	private Runnable runTask;
	private Callable<?> callTask;
	
	private long createdAtMillis;
	private WorkId key;
	private String topology;
	private int submissionCount;
	
	private volatile transient Object result;
    private volatile transient Exception e;
	
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
	
	public String getTopologyName() {
		return topology;
	}

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
    
    public Runnable getInnerRunnable() {
        return this.runTask;
    }
    
    public Callable<?> getInnerCallable() {
        return this.callTask;
    }

	public WorkId getWorkId() {
		return key;
	}

	
	
}
