package com.succinctllc.core.collections;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import com.succinctllc.core.collections.tracked.ITrackedQueue;
import com.succinctllc.executor.router.RoundRobinRouter;
import com.succinctllc.executor.router.RouteSkipAdapter;

public class PartitionedQueueRouter {
	public static interface PartitionRouter<E extends Partitionable> {
		public ITrackedQueue<E> nextPartition();
		public ITrackedQueue<E> peekPartition();
		public void setPartitionedQueueue(PartitionedQueue<E> queue);
	}
	
	public static class InOrderRouter<E extends Partitionable> implements PartitionRouter<E> {
		private PartitionedQueue<E> queue;
		
		public ITrackedQueue<E> peekPartition() {
			return nextPartition();
		}
		
		public ITrackedQueue<E> nextPartition() {
			return getOldestQueue();
		}
		
		protected ITrackedQueue<E> getOldestQueue(){
			long oldest = Long.MAX_VALUE;
			ITrackedQueue<E> oldestQueue = null;
			for(Entry<String, ITrackedQueue<E>> partitionQueue : this.queue.getPartitionMap().entrySet()) {
				ITrackedQueue<E> queue = partitionQueue.getValue();
				if(queue.getOldestTime() < oldest) {
					oldest = queue.getOldestTime();
					oldestQueue = queue;
				}
			}
			return oldestQueue;
		}

        public void setPartitionedQueueue(PartitionedQueue<E> queue) {
            this.queue = queue;
        }
		
	}
	
	
	public static class RoundRobinPartition<E extends Partitionable> implements PartitionRouter<E> {
		//private final AtomicReference lastPartition = new AtomicReference();
		private PartitionedQueue<E> queue;
		
		private RoundRobinRouter<String> router;
		
		
		public ITrackedQueue<E> nextPartition() {
		    String partition = router.next();
		    if(partition == null)
		        return null;
		    return queue.getPartition(partition);
		}	
		
		//TODO: it would be nice if peek really worked... but not necessary
		public ITrackedQueue<E> peekPartition() {
			return nextPartition();
		}

        public void setPartitionedQueueue(final PartitionedQueue<E> queue) {
            this.queue = queue;
            router = new RoundRobinRouter<String>(new Callable<List<String>>(){
                public List<String> call() throws Exception {
                    return queue.getPartitions();
                }}, new RouteSkipAdapter<String>() {
                    public boolean shouldSkip(String item) {
                        return queue.getPartition(item).size() == 0;
                    }
                });
        }
	}
	
	public static class WeightedPartitionRouter<E extends Partitionable> implements PartitionRouter<E> {
		public static interface PartitionWeigher<E extends Partitionable> {
			public long getWeight(String partition, PartitionedQueue<E> queue);
		}
		
		private final PartitionWeigher<E> weigher;
		public WeightedPartitionRouter(PartitionWeigher<E> weigher){
			this.weigher = weigher;
		}
		
		public ITrackedQueue<E> nextPartition() {
			// TODO Auto-generated method stub
			return null;
		}
		
		public ITrackedQueue<E> peekPartition() {
			return nextPartition();
		}

        public void setPartitionedQueueue(PartitionedQueue<E> queue) {
           
        }
		
	}
	
//	public static class WeightedFairRouter<E extends Partitionable & Weighted> implements PartitionRouter<E> {
//		public TrackedQueue<E> nextPartition(PartitionedQueue<E> queue) {
//			queue.get
//		}
//	}
}
