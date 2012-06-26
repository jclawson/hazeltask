package com.succinctllc.hazelcast.work.executor;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.succinctllc.core.concurrent.DefaultThreadFactory;
import com.succinctllc.core.concurrent.collections.grouped.GroupedPriorityQueue;
import com.succinctllc.core.concurrent.collections.grouped.GroupedQueueRouter;
import com.succinctllc.core.concurrent.collections.tracked.TrackedPriorityBlockingQueue.TimeCreatedAdapter;
import com.succinctllc.core.concurrent.executor.QueueExecutor;
import com.succinctllc.hazelcast.work.HazelcastWork;
import com.succinctllc.hazelcast.work.HazelcastWorkGroupedQueue;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;
import com.succinctllc.hazelcast.work.WorkResponse;
import com.succinctllc.hazelcast.work.executor.BoundedThreadPoolExecutorService.ExecutorListener;

public class LocalWorkExecutorService {

    private static ILogger LOGGER = Logger.getLogger(LocalWorkExecutorService.class.getName());
    
	private final HazelcastWorkTopology topology;
	//private BoundedThreadPoolExecutorService localExecutorService;
	private QueueExecutor localExecutorPool;
	private AtomicBoolean isStarted = new AtomicBoolean(false);
	//private HazelcastWorkGroupedQueue taskQueue;
	private GroupedPriorityQueue<HazelcastWork> taskQueue;
	//private final BlockingQueue<Runnable> localExecutorQueue;
	private final Map<String, HazelcastWork> worksInProgress;
	
	private final int maxThreads;
	
	protected LocalWorkExecutorService(HazelcastWorkTopology topology, int threadCount) {
		this.topology = topology;
		taskQueue = new HazelcastWorkGroupedQueue();
		this.maxThreads = threadCount;

//FIXME: why doesn't this work????
//		this.localExecutorQueue = new TrackedPriorityBlockingQueue<Runnable>(new TimeCreatedAdapter<Runnable>(){
//            public long getTimeCreated(Runnable item) {
//                return ((HazelcastWork)item).getTimeCreated();
//            }		    
//		});
		//this.localExecutorQueue = new LinkedBlockingQueue<Runnable>(maxThreads);
		DefaultThreadFactory factory = new DefaultThreadFactory("DistributedTask",topology.getName());     
		worksInProgress = new ConcurrentHashMap<String, HazelcastWork>();
		
		taskQueue = new GroupedPriorityQueue<HazelcastWork>(new GroupedQueueRouter.RoundRobinPartition<HazelcastWork>(),
                new TimeCreatedAdapter<HazelcastWork>(){
            public long getTimeCreated(HazelcastWork item) {
                return item.getTimeCreated();
            }            
        });
		
		localExecutorPool = new QueueExecutor<HazelcastWork>(taskQueue, maxThreads, factory);
	}

	public void start(){
		if(isStarted.compareAndSet(false, true)) {
			
			//note: if you don't write enough work to the linkedblockingqueue
			//      new threads will not be created
			//      you must exceed the queue size to see new threads
			//int blockingQueueSize = maxThreads*2;
//			localExecutorService = new BoundedThreadPoolExecutorService(
//			    0, maxThreads,
//		        60L, TimeUnit.SECONDS,
//		        localExecutorQueue,
//		        factory		                
//		    );
			
		    
		    localExecutorPool.addListener(new ResponseExecutorListener());
		    localExecutorPool.startup();
			//start copy queue thread
//			Thread t = factory.newNamedThread("QueueSync", new QueueSyncRunnable());
//			t.setDaemon(true);
//			t.start();
		}
	}
	
	private class ResponseExecutorListener implements ExecutorListener {
        public void afterExecute(Runnable runnable, Throwable exception) {
            //we finished this work... lets tell everyone about it!
            HazelcastWork work = (HazelcastWork)runnable;
            
            //remove from our local in-progress list
            //System.out.println("  -"+work.getUniqueIdentifier());
            worksInProgress.remove(work.getUniqueIdentifier());
            
            boolean success = exception == null && work.getException() == null;
            
            try {
                Member me = topology.getHazelcast().getCluster().getLocalMember();
                WorkResponse response;
                if(success) {
                    response = new WorkResponse(me, work.getUniqueIdentifier(), (Serializable)work.getResult(), WorkResponse.Status.SUCCESS);
                } else {
                    response = new WorkResponse(me, work.getUniqueIdentifier(), work.getException());
                }
                
                topology.getWorkResponseTopic().publish(response);
            } catch(RuntimeException e) {
                LOGGER.log(Level.SEVERE, "An error occurred while attempting to notify members of completed work", e);
            }
            
            //TODO: add task exceptions handling / retry logic
            //for now, just remove the work because its completed
            topology.getPendingWork()
                .remove(work.getUniqueIdentifier());
        }

        public void beforeExecute(Runnable runnable) {
            HazelcastWork work = (HazelcastWork)runnable;
            worksInProgress.put(work.getUniqueIdentifier(), work);
        }
	}
	
	/**
	 * This synchronizes our partitioned queue with the blocking queue that backs the executor service.
	 * It ensures that the blocking queue is never empty, and that enqueue operations into the partitioned
	 * queue remain fast
	 * 
	 * This runnable will poll from the partitioned queue and submit it to the executor service.  If the 
	 * partitioned queue is empty, it will wait 100ms to poll again with an exponential back off up to 15 seconds
	 * 
	 * @author jclawson
	 *
	 */
//	private class QueueSyncRunnable implements Runnable {
//		public void run() {
//			long minInterval = 100; 	//100 milliseconds
//			long interval = minInterval;
//			long exponent = 2;
//			int maxInterval = 10000;   //10 seconds
//			
//			while(!localExecutorService.isShutdown()) {
//				//long start = System.currentTimeMillis();
//			    HazelcastWork work = taskQueue.poll();
//				if(work != null) {
//					interval = minInterval;
//					
//					//System.out.println("  +"+work.getUniqueIdentifier());
//					worksInProgress.put(work.getUniqueIdentifier(), work);
//					//make sure we use execute so the work doesn't get wrapped
//					localExecutorService.execute(work);
//					
//					//System.out.println("   "+(System.currentTimeMillis()-start));
//					//do something with the future
//				} else { //there was no work in the taskQueue so lets wait a little
//					try {
//						Thread.sleep(interval);
//						interval = Math.min(maxInterval, interval * exponent);
//					} catch (InterruptedException e) {
//						Thread.currentThread().interrupt();
//					}
//				}
//			}
//		}		
//	}
	
	/**
	 * There are some race condition scenarios here.  The point is we want to get the most accurate time possible.
	 * We do some iterations, but the number of iterations is at most: (3 * number of threads) so its not that bad.
	 * 
	 * A race happens in the QueueSyncRunnable where we might not see the TRUE oldest time between when we poll() and put()
	 * into the worksInProgressMap.
	 * 
	 * An inaccurate result here may cause us to resubmit a work more than once if we think we lost it.  This isn't that bad.
	 * 
	 * TODO: investigate other options for keeping track of "oldest work time" in the local system so we know accurately what
	 * to recover.
	 * 
	 * @return
	 */
	public Long getOldestWorkCreatedTime(){
//	    Iterator<Runnable> localExecutorIt = localExecutorQueue.iterator();
	    long oldest = Long.MAX_VALUE;
//	    while(localExecutorIt.hasNext()) {
//	        HazelcastWork w = (HazelcastWork)localExecutorIt.next();
//	        if(w.getTimeCreated() < oldest) {
//	            oldest = w.getTimeCreated();
//	        }
//	    }
	    
	    //there is a little racecondition here where the item is polled from the queue
	    //but hasn't yet made it into the worksInProgress hashmap
	    for(HazelcastWork w : worksInProgress.values()) {
	        if(w.getTimeCreated() < oldest) {
	            oldest = w.getTimeCreated();
	        }
	    }
	    
	    Long oldestQueueTime = this.taskQueue.getOldestQueueTime();
	    
	    if(oldestQueueTime != null && oldestQueueTime < oldest)
	        oldest = oldestQueueTime;
	    
	    return oldest;
	}
	
	public long getQueueSize() {
	    return this.taskQueue.size();
	}
	
	public void execute(HazelcastWork command) {
	    taskQueue.add(command);
	}

	//FIXME: fix this
	public void shutdown() {
		localExecutorPool.shutdownNow();
	}
	
	//FIXME: fix this
	public List<Runnable> shutdownNow() {
	    localExecutorPool.shutdownNow();
	    return Collections.emptyList();
	}

	public boolean isShutdown() {
		return localExecutorPool.isShutdown();
	}

//	//FIXME: fix this
//	public boolean isTerminated() {
//		return localExecutorPool.isShutdown();
//	}

}
