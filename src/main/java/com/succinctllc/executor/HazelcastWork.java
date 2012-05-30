package com.succinctllc.executor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.core.PartitionAware;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.Serializer;
import com.succinctllc.core.collections.Partitionable;

//TODO: make sure Runnable task is data serializable
public class HazelcastWork implements Partitionable, PartitionAware<String>, Runnable, WorkKeyable, DataSerializable {
	private static final long serialVersionUID = 1L;
	
	private Runnable task;
	private long createdAtMillis;
	private WorkKey key;
	
	public HazelcastWork(WorkKey key, Runnable task){
		this.task = task;
		this.key = key;
		createdAtMillis = System.currentTimeMillis();
	}

	public String getPartition() {
		return key.getLocalPartition();
	}
	
	public String getPartitionKey() {
		return key.getHazelcastPartition();
	}

	public String getUniqueIdentifier() {
		return key.getId();
	}
	
	public long getTimeCreated(){
		return createdAtMillis;
	}

	public void run() {
		task.run();
	}

	public WorkKey getKey() {
		return key;
	}

	public void writeData(DataOutput out) throws IOException {
		new Serializer().writeObject(task).writeData(out);
		key.writeData(out);
		out.writeLong(createdAtMillis);
	}

	public void readData(DataInput in) throws IOException {
		Data data = new Data();
		data.readData(in);
		task = (Runnable) new Serializer().readObject(data);
		
		key = new WorkKey();
		key.readData(in);
		
		createdAtMillis = in.readLong();
	}

	
	
}
