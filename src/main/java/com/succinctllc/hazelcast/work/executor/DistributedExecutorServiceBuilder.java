package com.succinctllc.hazelcast.work.executor;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;
import com.succinctllc.hazelcast.work.WorkKeyAdapter;

public class DistributedExecutorServiceBuilder {
	
	public static DistributedExecutorService dInstance;
	public static LocalWorkExecutorService lInstance;
	public static AtomicInteger atomicServiceIndex = new AtomicInteger(1);
	
	public static enum DistributionType {
	    MAP,
	    TOPIC
	};
	
	public static class InternalBuilderStep1 {
	    private final HazelcastInstance hazelcast;
	    public InternalBuilderStep1(HazelcastInstance hazelcast) {
           this.hazelcast = hazelcast;
        }

        /**
	     * We must be able to, given a piece of work, identify its WorkKey
	     * 
	     * @param adapter
	     * @return
	     */
	    public InternalBuilderStep2 withWorkKeyAdapter(WorkKeyAdapter adapter){
	        return new InternalBuilderStep2(hazelcast, adapter);
	    }
	}
	
	public static class InternalBuilderStep2 {
	    protected final int serviceIndex;
	    protected HazelcastWorkTopology topology;
	    protected DistributionType type;
	    protected WorkKeyAdapter partitionAdapter;
	    protected final HazelcastInstance hazelcast;
	    
	    public InternalBuilderStep2(HazelcastInstance hazelcast, WorkKeyAdapter partitionAdapter){
	        serviceIndex = atomicServiceIndex.getAndIncrement();
	        type = DistributionType.TOPIC;
	        this.partitionAdapter = partitionAdapter;
	        this.hazelcast = hazelcast;
	    }
	    
	    public InternalBuilderStep2 withWorkKeyAdapter(WorkKeyAdapter adapter){
	        this.partitionAdapter = adapter;
	        return this;
        }
	    
	    public InternalBuilderStep2 withTopology(HazelcastWorkTopology topology){
	        this.topology = topology;
	        return this;
	    }
	    
//	    public InternalBuilderStep2 withDistributionType(DistributionType type) {
//	        this.type = type;
//	        return this;
//	    }
	    
	    public DistributedExecutorService build() {
	        if(this.topology == null)
	        	this.topology = HazelcastWorkTopology.getDefault(hazelcast);
	    	return new DistributedExecutorService(this);
	    }
	}
	
	public static InternalBuilderStep1 builder(){
        return new InternalBuilderStep1(Hazelcast.getDefaultInstance());
    }
	
	public static InternalBuilderStep1 builder(HazelcastInstance hazelcast){
	    return new InternalBuilderStep1(hazelcast);
	}
}
