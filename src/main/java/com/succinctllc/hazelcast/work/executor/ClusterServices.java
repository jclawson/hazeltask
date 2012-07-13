package com.succinctllc.hazelcast.work.executor;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.hazelcast.core.Member;
import com.succinctllc.hazelcast.util.MemberTasks;
import com.succinctllc.hazelcast.util.MemberTasks.MemberResponse;
import com.succinctllc.hazelcast.work.HazelcastWork;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;


/**
 * This class abstracts the cluster wide method calls
 * to retrieve data
 * 
 * @author jclawson
 */
public class ClusterServices {
	
	private HazelcastWorkTopology topology;
	
	public ClusterServices(HazelcastWorkTopology topology) {
		this.topology = topology;
	}
	
	public Collection<MemberResponse<Long>> getLocalQueueSizes() {
		return MemberTasks.executeOptimistic(
			topology.getCommunicationExecutorService(), 
			topology.getHazelcast().getCluster().getMembers(), 
			new GetLocalQueueSizes(topology.getName())
		);
	}
	
	public boolean submitWork(HazelcastWork work, Member member, boolean waitForAcknowledgement) {
		@SuppressWarnings("unchecked")
		Future<Boolean> future = (Future<Boolean>) topology.getWorkDistributor().submit(MemberTasks.create(new SubmitWorkTask(work, topology.getName()), member));
		if(waitForAcknowledgement) {
			try {
				return future.get();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			} catch (ExecutionException e) {
				return false;
			}
		} else {
			return true;
		}
	}
	
	protected static class GetLocalQueueSizes extends AbstractCallable<Long> {
		private static final long serialVersionUID = 1L;

		public GetLocalQueueSizes(String topology) {
			super(topology);
		}

		public Long call() throws Exception {
			return svc.getLocalExecutorService().getQueueSize();
		}		
	}
	
	public class SubmitWorkTask extends AbstractCallable<Boolean> {
	    private static final long serialVersionUID = 1L;
	    private HazelcastWork work;
	    
	    public SubmitWorkTask(HazelcastWork work, String topology) {
	        super(topology);
	    	this.work = work;
	    }
	    
	    public Boolean call() throws Exception {
	        svc.getLocalExecutorService().execute(work);
	        return true;
	    }
	}
	
	protected abstract static class AbstractCallable<T> implements Callable<T>, DistributedExecutorServiceAware, Serializable {
		private static final long serialVersionUID = 1L;
		private final String topology;
		protected DistributedExecutorService svc;
		
		public AbstractCallable(String topology) {
			this.topology = topology;
		}

		public String getTopology() {
			return topology;
		}

		public void setDistributedExecutorService(DistributedExecutorService svc) {
			this.svc = svc;
		}	
	}
}
