package com.succinctllc.hazelcast.work.executor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.hazelcast.core.Member;
import com.succinctllc.hazelcast.data.MemberValuePair;
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
			topology.getReadyMembers(),
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
	
	@SuppressWarnings("unchecked")
    public Collection<HazelcastWork> stealTasks(Collection<MemberValuePair<Long>> values) {
	    Collection<HazelcastWork> result = new LinkedList<HazelcastWork>();
	    Collection<Future<Collection<HazelcastWork>>> futures = new ArrayList<Future<Collection<HazelcastWork>>>(values.size());
	    for(MemberValuePair<Long> entry : values) {
	        futures.add((Future<Collection<HazelcastWork>>)topology.getCommunicationExecutorService()
	            .submit(MemberTasks.create(new StealTasks(topology.getName(), entry.getValue()), entry.getMember())));
	    }
	    
	    for(Future<Collection<HazelcastWork>> f : futures) {
	        try {
                Collection<HazelcastWork> work = f.get(3, TimeUnit.MINUTES);//wait at most 3 minutes
                result.addAll(work);
            } catch (InterruptedException e) {
                //FIXME: log... we may have just dumped work into the ether.. it will have to be recovered
                //this really really should not happen
                Thread.currentThread().interrupt();
                return result;
            } catch (ExecutionException e) {
                //FIXME: log... we may have just dumped work into the ether.. it will have to be recovered
                continue;
            } catch (TimeoutException e) {
                //FIXME: log error... we just dumped work into the ether.. it will have to be recovered
                continue;
            } 
	    }
	    return result;
	}
	
	protected static class StealTasks extends AbstractCallable<Collection<HazelcastWork>> {
        private static final long serialVersionUID = 1L;
        
        private long numberOfTasks;
	    
        public StealTasks(String topology, long numberOfTasks) {
            super(topology);
            this.numberOfTasks = numberOfTasks;
        }

        public Collection<HazelcastWork> call() throws Exception {
            return this.svc.getLocalExecutorService().stealTasks(numberOfTasks);
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
	
	public static class SubmitWorkTask extends AbstractCallable<Boolean> {
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
