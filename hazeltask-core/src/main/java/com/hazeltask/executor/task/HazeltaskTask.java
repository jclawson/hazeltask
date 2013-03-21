package com.hazeltask.executor.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.SerializationHelper;
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
	private int submissionCount;
	private transient HazelcastInstance hazelcastInstance;
	
	private volatile transient Object result;
    private volatile transient Exception e;
	
    //required for DataSerializable
    protected HazeltaskTask(){}
    
	public HazeltaskTask(UUID id, G group, Runnable task){
		this.runTask = task;
		this.id = id;
		this.group = group;
		createdAtMillis = System.currentTimeMillis();
		this.submissionCount = 1;
	}
	
	public HazeltaskTask(UUID id, G group, Callable<?> task){
        this.callTask = task;
        this.id = id;
        this.group = group;
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
    public void writeData(DataOutput out) throws IOException {
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
        
        SerializationHelper.writeObject(out, group);
        SerializationHelper.writeObject(out, runTask);
        SerializationHelper.writeObject(out, callTask);
        out.writeLong(createdAtMillis);
        out.writeInt(submissionCount);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readData(DataInput in) throws IOException {
        long m = in.readLong();
        long l = in.readLong();
        
        id = new UUID(m, l);
        group = (G) SerializationHelper.readObject(in);
        runTask = (Runnable) SerializationHelper.readObject(in);
        callTask = (Callable<?>) SerializationHelper.readObject(in);
        
        createdAtMillis = in.readLong();
        submissionCount = in.readInt();
    }
	
}
