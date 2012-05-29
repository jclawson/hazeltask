package com.succinctllc.executor;

import com.hazelcast.core.PartitionAware;
import com.succinctllc.core.collections.Partitionable;

public class WorkKey implements PartitionAware<String> {
	private final String uniqueId;
	private final long createdAtMillis;
	
	public static WorkKey createFor(Partitionable item){
		return new WorkKey(item.getUniqueIdentifier());
	}
	
	private WorkKey(String id) {
		uniqueId = id;
		this.createdAtMillis = System.currentTimeMillis();
	}
	
	public String getPartitionKey() {
		return uniqueId;
	}
}
