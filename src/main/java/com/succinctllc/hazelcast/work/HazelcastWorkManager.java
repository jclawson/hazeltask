package com.succinctllc.hazelcast.work;

import com.succinctllc.hazelcast.work.bundler.DeferredWorkBundler;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorService;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorServiceManager;

/**
 * This util class helps get at the work services created through their respective builders.  
 * Remote tasks must be able to get a hold of their topology's local service instance
 * to perform certain tasks.  This requires the ability to fetch these instances by 
 * topology.  
 * 
 * @author Jason Clawson
 */
public class HazelcastWorkManager {
	 
	 public static HazelcastWorkTopology getTopology(String name) {
		 return HazelcastWorkTopology.get(name);
	 }
	 
	 public static DistributedExecutorService getDistributedExecutorService(String topologyName) {
		 return DistributedExecutorServiceManager
				 	.getDistributedExecutorServiceManager(topologyName)
				 	.getDistributedExecutorService();
	 }
	 
	 public static <T> DeferredWorkBundler<T> getDeferredWorkBundler(String topologyName) {
		 return DeferredWorkBundler.<T>getDeferredWorkBundler(topologyName);
	 }
	 
}
