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
	    protected boolean acknowlegeWorkSubmission = false;
	    protected boolean disableWorkers = false;
	    protected int threadCount = 4;
	    
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
	    
	    /**
	     * Work is submitted via Hazelcast executor service to a target node. By default
	     * we will NOT wait until the node has acknowleged that it received it and has put it into
	     * its grouped queue.  Enable this option if you want the ability to retry the submit 
	     * if the target node goes down while submitting.  Enabling this option will slow down
	     * enqueue operations.
	     * 
	     * You will NOT lose work with ACK turned off.  Worst case, it will be worked on later 
	     * when the recovery process sees that there is work in the pending map that doesn't 
	     * exist on any node.
	     * 
	     * @return
	     */
	    public InternalBuilderStep2<W> withWorkSubmissionAcknowledgement() {
	        this.acknowlegeWorkSubmission = true;
	        return this;
	    }
	    
	    /**
	     * disableWorkers if you do not want this node to execute any work
	     * @return
	     */
	    public InternalBuilderStep2<W> disableWorkers() {
            this.disableWorkers = true;
            return this;
        }
	    
	    public InternalBuilderStep2<W> withThreadCount(int numberOfThreads) {
	        this.threadCount = numberOfThreads;
	        return this;
	    }
	}
	
	public static InternalBuilderStep1 builder(String topologyName){
        return builder(Hazelcast.getDefaultInstance(), topologyName);
    }
	
	public static InternalBuilderStep1 builder(HazelcastInstance hazelcast, String topologyName){
	    return new InternalBuilderStep1(hazelcast, topologyName);
	}
}
