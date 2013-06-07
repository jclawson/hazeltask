package com.hazeltask.executor.task;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;

import lombok.extern.slf4j.Slf4j;

import com.hazelcast.core.Member;
import com.hazeltask.HazeltaskTopology;
import com.hazeltask.core.concurrent.BackoffTimer.BackoffTask;
import com.hazeltask.executor.IExecutorTopologyService;
import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.metrics.ExecutorMetrics;
import com.hazeltask.hazelcast.MemberTasks.MemberResponse;
import com.hazeltask.hazelcast.MemberValuePair;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 * Is there a way we can do this without locking?
 * 
 * We will only take work if we find a node that has PERCENT_THRESHOLD more work than 
 * this node.
 * 
 * We lock these tasks with a cluster wide lock so that only one per node may run
 * 
 * TODO: lets have TaskStealPolicies so this is customizable, when and how much to steal
 * 
 * @author jclawson
 */
@Slf4j
public class TaskRebalanceTimerTask<GROUP extends Serializable> extends BackoffTask {
    private final Member localMember;
    private final IExecutorTopologyService<GROUP> executorTopologyService;
    private final LocalTaskExecutorService<GROUP> localSvc;
    
    private Histogram histogram;
    private Timer redistributionTimer;
    private Timer lockWaitTimer;
    private Counter getRebalanceNoopCounter;
	
	/**
	 * If a member has this percent MORE works than the current member, steal them
	 */
	private static final double THRESHOLD = 0.30;
	
	private final Lock LOCK;
	
	public TaskRebalanceTimerTask(HazeltaskTopology<GROUP> topology, LocalTaskExecutorService<GROUP> localSvc, IExecutorTopologyService<GROUP> executorTopologyService, ExecutorMetrics metrics) {
	    LOCK = executorTopologyService.getRebalanceTaskClusterLock();
		localMember = topology.getLocalMember();
		this.executorTopologyService = executorTopologyService;
		this.localSvc = localSvc;
		
		histogram = metrics.getTaskBalanceHistogram().getMetric();
        redistributionTimer = metrics.getTaskBalanceTimer().getMetric();
        lockWaitTimer = metrics.getTaskBalanceLockWaitTimer().getMetric();
        getRebalanceNoopCounter = metrics.getRebalanceNoopCounter().getMetric();
	}
	
	
	@Override
    public boolean execute() {
	    try {
    	    log.debug( "Running Rebalance Task");
    	    TimerContext waitCtx = lockWaitTimer.time();
    	    try {    	        
    	        LOCK.lock();
    	    } finally {
    	        //TODO: refactor this so its cleaner
    	        try {
    	            waitCtx.stop();
    	        } catch (Throwable t) {
    	            //not sure if this is possible
    	            LOCK.unlock();
    	            throw t;//outer catch should get this.
    	        }
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
        	    Collection<MemberResponse<Long>> queueSizes = executorTopologyService.getMemberQueueSizes();
        	    if(queueSizes.size() == 0) {
        	        getRebalanceNoopCounter.inc();
        	        return false;
        	    }
        		
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
        		    //TODO: throw exception?
        		    log.error( "Cannot get localQueueSize");
        		    return false;
        		}	
        		
        		if(localQueueSize >= optimalSize || (optimalSize * THRESHOLD) <= localQueueSize) {
        		    //nothing to do
        		    log.info( "No rebalance needed");
        		    getRebalanceNoopCounter.inc();
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
        		
        		log.info( "Total Size: "+totalSize+", Optimal size: "+optimalSize+", Local Size: "+localQueueSize);
        		log.info( "I will take "+needToTake+" tasks from "+numToTake.size()+" nodes");
        		
        		//------------------------
        		// Take a percentage according to the total available to take
        		//------------------------
        		for(MemberResponse<Long> response : queueSizes) {
        		    if(response.getValue() > optimalSize) {
        		        double percent = ((double)response.getValue() / (double)totalExceedingIdeal);
        		        long take = (long) Math.round(needToTake * percent);
        		    	numToTake.add(new MemberValuePair<Long>(response.getMember(), take));
        		    	log.info( "I will take "+take+" tasks from "+response.getMember());
        		    }
        		}
    		
        		
    		//for each numToTake, send a message to steal work
    		//use a completion service to manage futures and recieve results
        	//make sure to bound the waiting of each call with something like 5 minutes or 10 minutes
        		
        		//TODO: replace this with a completion service so we can process results as we get them
        		Collection<HazeltaskTask<GROUP>> stolenTasks = executorTopologyService.stealTasks(numToTake);
        		//add to local queue
        		int totalAdded = 0;
        		for(HazeltaskTask<GROUP> task : stolenTasks) {
        		    localSvc.execute(task);
        		    totalAdded++;
        		}
        		
        		if(histogram != null)
        		    histogram.update(totalAdded);
        		
        		log.info( "Done adding "+totalAdded+"...");
        		
        		
    	    } finally {
    	        try {
    	            LOCK.unlock();
    	        } finally {
    	            timerCtx.stop();
    	        }
    	    }
            return false;
	    } catch (Throwable t) {
	        //catch all exceptions and swallow so it doens't cancel our timer task
	        log.error( "Error running Rebalance Task", t);
	        return true;
	    }
	}
}
