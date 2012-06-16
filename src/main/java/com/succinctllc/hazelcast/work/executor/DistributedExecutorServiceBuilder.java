package com.succinctllc.hazelcast.work.executor;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;
import com.succinctllc.hazelcast.work.WorkIdAdapter;

public class DistributedExecutorServiceBuilder {
	
	public static class InternalBuilderStep1 {
	    private final HazelcastWorkTopology topology;
	    public InternalBuilderStep1(HazelcastInstance hazelcast, String topologyName) {
           this.topology = HazelcastWorkTopology.getOrCreate(topologyName, hazelcast);
        }

        /**
	     * We must be able to, given a piece of work, identify its WorkKey
	     * 
	     * @param adapter
	     * @return
	     */
	    public <W> InternalBuilderStep2<W> withWorkKeyAdapter(WorkIdAdapter<W> adapter){
	        return new InternalBuilderStep2<W>(topology, adapter);
	    }
	}
	
	public static class InternalBuilderStep2<W> {
	    protected HazelcastWorkTopology topology;
	    protected WorkIdAdapter<W> partitionAdapter;
	    
	    public InternalBuilderStep2(HazelcastWorkTopology topology, WorkIdAdapter<W> partitionAdapter){
	        this.partitionAdapter = partitionAdapter;
	        this.topology = topology;
	    }
	    
	    public InternalBuilderStep2<W> withWorkKeyAdapter(WorkIdAdapter<W> adapter){
	        this.partitionAdapter = adapter;
	        return this;
        }
	    
	    public DistributedExecutorService build() {
	    	return new DistributedExecutorService(this);
	    }
	}
	
	public static InternalBuilderStep1 builder(String topologyName){
        return builder(Hazelcast.getDefaultInstance(), topologyName);
    }
	
	public static InternalBuilderStep1 builder(HazelcastInstance hazelcast, String topologyName){
	    return new InternalBuilderStep1(hazelcast, topologyName);
	}
}
