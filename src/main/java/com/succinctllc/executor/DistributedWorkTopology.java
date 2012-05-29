package com.succinctllc.executor;

import java.util.UUID;

import com.hazelcast.core.IMap;

public class DistributedWorkTopology {
	public String name;
	public IMap<WorkKey, Runnable> map;
	public UUID jvmId = UUID.randomUUID();
	public long jvmNanos = System.nanoTime();
}
