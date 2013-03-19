package com.hazeltask.executor.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
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
public class HazeltaskTask<ID extends Serializable, G extends Serializable> 
    implements Runnable, Task<ID,G>, HazelcastInstanceAware, TrackCreated {
	private static final long serialVersionUID = 1L;
	
	private Runnable runTask;
	private Callable<?> callTask;
	
	private long createdAtMillis;
	private ID id;
	private G group;
	private String topology;
	private int submissionCount;
	private transient HazelcastInstance hazelcastInstance;
	
	private volatile transient Object result;
    private volatile transient Exception e;
	
    //required for DataSerializable
    protected HazeltaskTask(){}
    
	public HazeltaskTask(String topology, ID id, G group, Runnable task){
		this.runTask = task;
		this.id = id;
		this.group = group;
		this.topology = topology;
		createdAtMillis = System.currentTimeMillis();
		this.submissionCount = 1;
	}
	
	public HazeltaskTask(String topology, ID id, G group, Callable<?> task){
        this.callTask = task;
        this.id = id;
        this.group = group;
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
    public ID getId() {
        return id;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }
    
    @Override
    public void writeData(DataOutput out) throws IOException {
        SerializationHelper.writeObject(out, id);
        SerializationHelper.writeObject(out, group);
        SerializationHelper.writeObject(out, runTask);
        SerializationHelper.writeObject(out, callTask);
        out.writeLong(createdAtMillis);
        out.writeUTF(topology);
        out.writeInt(submissionCount);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readData(DataInput in) throws IOException {
        id = (ID) SerializationHelper.readObject(in);
        group = (G) SerializationHelper.readObject(in);
        runTask = (Runnable) SerializationHelper.readObject(in);
        callTask = (Callable<?>) SerializationHelper.readObject(in);
        
        createdAtMillis = in.readLong();
        topology = in.readUTF();
        submissionCount = in.readInt();
    }
	
}
