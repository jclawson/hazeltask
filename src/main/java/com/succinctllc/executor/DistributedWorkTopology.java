package com.succinctllc.executor;

import java.util.UUID;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class DistributedWorkTopology {
	public String name;
	public IMap<String, HazelcastWork> map;
	public WorkKeyAdapter partitionAdapter;
	public UUID jvmId = UUID.randomUUID();
	public long jvmNanos = System.nanoTime();
	public HazelcastInstance hazelcast;
}
