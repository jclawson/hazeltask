package com.hazeltask.executor.task;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.codahale.metrics.Timer;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazeltask.core.concurrent.collections.tracked.TrackCreated;

/**
 * This class wraps a runnable and provides other metadata we need to searching work items
 * in the distributed map.
 * 
 * @author jclawson
 *
 */
public class HazeltaskTask< G extends Serializable> 
    implements Runnable, Task<G>, HazelcastInstanceAware, TrackCreated {
	private static final long serialVersionUID = 1L;
	
	private Runnable runTask;
	private Callable<?> callTask;
	
	private long createdAtMillis;
	private UUID id;
	private G group;
	private Serializable taskInfo;
	
	private int submissionCount;
	private transient HazelcastInstance hazelcastInstance;
	private transient Timer taskExecutedTimer;
	
	private volatile transient Object result;
    private volatile transient Exception e;
	
    //required for DataSerializable
    protected HazeltaskTask(){}
    
	public HazeltaskTask(UUID id, G group, Serializable taskInfo, Runnable task){
		this.runTask = task;
		this.id = id;
		this.group = group;
		createdAtMillis = System.currentTimeMillis();
		this.submissionCount = 1;
		this.taskInfo = taskInfo;
	}
	
	public HazeltaskTask(UUID id, G group, Serializable taskInfo, Callable<?> task){
        this.callTask = task;
        this.id = id;
        this.group = group;
        createdAtMillis = System.currentTimeMillis();
        this.submissionCount = 1;
        this.taskInfo = taskInfo;
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

	public G getGroup() {
		return group;
	}

	public Object getResult() {
        return result;
    }

    public Exception getException() {
        return e;
    }
	
	public long getTimeCreated(){
		return createdAtMillis;
	}

    public void run() {
        Timer.Context ctx = null;
        if(taskExecutedTimer != null)
            ctx = taskExecutedTimer.time();
        try {            
            if(callTask != null) {
    		    if(callTask instanceof HazelcastInstanceAware) {
    		        ((HazelcastInstanceAware) callTask).setHazelcastInstance(hazelcastInstance);
    		    }
                this.result = callTask.call();
    		} else {
    		    if(runTask instanceof HazelcastInstanceAware) {
                    ((HazelcastInstanceAware) runTask).setHazelcastInstance(hazelcastInstance);
                }
    		    runTask.run();
    		}
        } catch (Exception t) {
            this.e = t;
        } finally {
            if(ctx != null)
                ctx.stop();
        }
	}
    
    public Runnable getInnerRunnable() {
        return this.runTask;
    }
    
    public Callable<?> getInnerCallable() {
        return this.callTask;
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }
    
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
        
        out.writeObject(group);
        out.writeObject(runTask);
        out.writeObject(callTask);
        
        out.writeLong(createdAtMillis);
        out.writeInt(submissionCount);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readData(ObjectDataInput in) throws IOException {
        long m = in.readLong();
        long l = in.readLong();
        
        id = new UUID(m, l);
        group = (G) in.readObject();
        runTask = (Runnable) in.readObject();
        callTask = (Callable<?>) in.readObject();
        
        createdAtMillis = in.readLong();
        submissionCount = in.readInt();
    }

    public void setExecutionTimer(Timer taskExecutedTimer) {
        this.taskExecutedTimer = taskExecutedTimer;
    }

    public Serializable getTaskInfo() {
        return taskInfo;
    }
	
}
