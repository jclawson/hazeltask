package com.succinctllc.executor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.DataSerializable;


public class WorkKey implements DataSerializable {
	private static final long serialVersionUID = 1L;
	
	private String uniqueId;
	private String localPartition;
	private String hazelcastPartition;
		
	protected WorkKey(String id, String hazelcastPartition, String localPartition) {
		this.uniqueId = id;
		this.localPartition = localPartition;
		this.hazelcastPartition = hazelcastPartition;
	}
	
	protected WorkKey(){}
	
	public String getHazelcastPartition() {
		return hazelcastPartition;
	}
	
	public String getLocalPartition() {
		return localPartition;
	}
	
	public String getId(){
		return uniqueId;
	}

	public void writeData(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void readData(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	
	
}
