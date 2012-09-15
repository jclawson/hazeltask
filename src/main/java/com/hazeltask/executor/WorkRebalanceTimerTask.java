package com.hazeltask.executor;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazeltask.HazeltaskTopology;
import com.hazeltask.core.concurrent.BackoffTimer.BackoffTask;
import com.hazeltask.hazelcast.MemberTasks.MemberResponse;
import com.hazeltask.hazelcast.MemberValuePair;
import com.succinctllc.hazelcast.work.HazelcastWork;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 * TODO: for this, lets lock the cluster down so we can figure out exactly 
 * what to do without other memebers running this task and altering the metrics
 * what would happen if we didn't lock?
 * 
 * We will only take work if we find a node that has PERCENT_THRESHOLD more work than 
 * this node.
 * 
 * We lock these tasks with a cluster wide lock so that only one per node may run
 * 
 * FIXME: lets have TaskStealPolicies so this is customizable, when and how much to steal
 * 
 * @author jclawson
 */
public class WorkRebalanceTimerTask extends BackoffTask {
    private static ILogger LOGGER = Logger.getLogger(WorkRebalanceTimerTask.class.getName());
    private final Member localMember;
    private final IExecutorTopologyService executorTopologyService;
    
    private Histogram histogram;
    private Timer redistributionTimer;
    private Timer lockWaitTimer;
	
	/**
	 * If a member has this percent MORE works than the current member, steal them
	 */
	private static final double THRESHOLD = 0.30;
	
	private final Lock LOCK;
	
	public WorkRebalanceTimerTask(HazeltaskTopology topology, IExecutorTopologyService executorTopologyService) {
	    ExecutorMetrics metrics = topology.getExecutorMetrics();
	    LOCK = executorTopologyService.getRebalanceTaskClusterLock();
		localMember = topology.getHazeltaskConfig().getHazelcast().getCluster().getLocalMember();
		this.executorTopologyService = executorTopologyService;
		
		histogram = metrics.getTaskBalanceHistogram().getMetric();
        redistributionTimer = metrics.getTaskBalanceTimer().getMetric();
        lockWaitTimer = metrics.getTaskBalanceLockWaitTimer().getMetric();
	}
	
	
	@Override
    public boolean execute() {
	    LOGGER.log(Level.INFO, "Running Rebalance Task");
	    TimerContext waitCtx = lockWaitTimer.time();
	    try {
	        
	        LOCK.lock();
	    } finally {
	        waitCtx.stop();
	    }
	    /*
	     * NOTE: because we are locking here, we need to make ABSOLUTELY sure all our waits are bounded
	     * ----> Comment on any external calls to note that they are bounded
	     */
	    TimerContext timerCtx = null; 
	    try {
	        timerCtx = redistributionTimer.time();
	        //ClusterServices clusterServices = distributedExecutorService.getTopology().getClusterServices();
    		
    	    //BOUNDED: MemberTasks.executeOptimistic waits a max of 60 seconds
    	    Collection<MemberResponse<Long>> queueSizes = executorTopologyService.getLocalQueueSizes();
    	    if(queueSizes.size() == 0) {
    	        LOGGER.log(Level.INFO, "No data");
    	        return false;
    	    }
    		
    	    //TODO: check if response.getMember().localMember() works below so we don't have to do this
    	    //Member localMember = this.distributedExecutorService.getTopology().getHazelcast().getCluster().getLocalMember();
            long localQueueSize = -1;        
    		long totalSize = 0;
    	
    		for(MemberResponse<Long> response : queueSizes) {
    			totalSize += response.getValue();
    			if(response.getMember().equals(localMember)) {
    			    localQueueSize = response.getValue();
    			}
    		}
    		
    		final long optimalSize = totalSize / queueSizes.size();
    		
    		if(localQueueSize == -1) {
    		    //the localQueueSize was not fetched for some reason... 
    		    //TODO: throw exception
    		    LOGGER.log(Level.SEVERE, "Cannot get localQueueSize");
    		    return false;
    		}	
    		
    		if(localQueueSize >= optimalSize || (optimalSize * THRESHOLD) <= localQueueSize) {
    		    //nothing to do
    		    //TODO: log
    		    LOGGER.log(Level.INFO, "No rebalance needed");
    		    return false;
    		}
    		
    		//------------------------
    		// Figure out how much is available to take.. (sum of those that exceed the average size)
    		//------------------------
    		long totalExceedingIdeal = 0;
    		for(MemberResponse<Long> response : queueSizes) {
    		    if(response.getValue() > optimalSize)
    		        totalExceedingIdeal += response.getValue();
    		}
    		
    		final long needToTake =  optimalSize - localQueueSize;
    		LinkedList<MemberValuePair<Long>> numToTake = new LinkedList<MemberValuePair<Long>>();
    		
    		LOGGER.log(Level.INFO, "Total Size: "+totalSize+", Optimal size: "+optimalSize+", Local Size: "+localQueueSize);
    		LOGGER.log(Level.INFO, "I will take "+needToTake+" tasks from "+numToTake.size()+" nodes");
    		
    		//------------------------
    		// Take a percentage according to the total available to take
    		//------------------------
    		for(MemberResponse<Long> response : queueSizes) {
    		    if(response.getValue() > optimalSize) {
    		        double percent = ((double)response.getValue() / (double)totalExceedingIdeal);
    		        long take = (long) Math.round(needToTake * percent);
    		    	numToTake.add(new MemberValuePair<Long>(response.getMember(), take));
    		    	LOGGER.log(Level.INFO, "I will take "+take+" tasks from "+response.getMember());
    		    }
    		}
		
    		
		//for each numToTake, send a message to steal work
		//use a completion service to manage futures and recieve results
    	//make sure to bound the waiting of each call with something like 5 minutes or 10 minutes
    		
    		//TODO: replace this with a completion service so we can process results as we get them
    		Collection<HazelcastWork> stolenTasks = executorTopologyService.stealTasks(numToTake);
    		//add to local queue
    		int totalAdded = 0;
    		for(HazelcastWork task : stolenTasks) {
    		    executorTopologyService.addTaskToLocalQueue(task); //TODO: what if it returns false?
    		    totalAdded++;
    		}
    		
    		if(histogram != null)
    		    histogram.update(totalAdded);
    		
    		LOGGER.log(Level.INFO, "Done adding "+totalAdded+"...");
    		
    		
	    } finally {
	        try {
	            LOCK.unlock();
	        } finally {
	            timerCtx.stop();
	        }
	    }
        return false;
	}
}
