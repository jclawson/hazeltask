package com.succinctllc.hazelcast.work;

import java.io.Serializable;

import com.hazelcast.core.PartitionAware;
import com.succinctllc.core.concurrent.collections.grouped.Groupable;

//TODO: make sure Runnable task is data serializable
public class HazelcastWork implements Groupable, PartitionAware<String>, Runnable, WorkKeyable, Serializable {
	private static final long serialVersionUID = 1L;
	
	private Runnable task;
	private long createdAtMillis;
	private WorkReference key;
	private String topology;
	private int submissionCount;
	
	//TODO: store jvmId & nanotime offset so we can better order items on recovery
	//.... actually order doesn't really matter that much since we have multiple threads
	//processing stuff.  Unless you want a single thread processing a queue at a time across
	//all nodes... order doesn't matter
	
	public HazelcastWork(String topology, WorkReference key, Runnable task){
		this.task = task;
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
	
	public Runnable getDelegate(){
	    return task;
	}

	public String getGroup() {
		return key.getGroup();
	}
	
	public String getPartitionKey() {
		return key.getPartitionKey();
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
		    //TODO: add task exceptions handling / retry logic
		    //for now, just remove the work becaues its completed    
		    HazelcastWorkTopology.get(topology)
		        .getPendingWork()
		        .remove(key.getId());
		}
	}

	public WorkReference getKey() {
		return key;
	}

	
	
}
