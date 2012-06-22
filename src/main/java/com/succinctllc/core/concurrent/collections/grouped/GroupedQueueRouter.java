package com.succinctllc.core.concurrent.collections.grouped;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import com.succinctllc.core.concurrent.collections.tracked.ITrackedQueue;
import com.succinctllc.core.concurrent.collections.tracked.TrackedQueue;
import com.succinctllc.hazelcast.work.router.RoundRobinRouter;
import com.succinctllc.hazelcast.work.router.RouteSkipAdapter;

public class GroupedQueueRouter {
	public static interface GroupedRouter<E extends Groupable> {
		public ITrackedQueue<E> nextPartition();
		public ITrackedQueue<E> peekPartition();
		public void setPartitionedQueueue(IGroupedQueue<E> queue);
	}
	
	public static class InOrderRouter<E extends Groupable> implements GroupedRouter<E> {
		private IGroupedQueue<E> queue;
		
		public ITrackedQueue<E> peekPartition() {
			return nextPartition();
		}
		
		public ITrackedQueue<E> nextPartition() {
			return getOldestQueue();
		}
		
		protected ITrackedQueue<E> getOldestQueue(){
			long oldest = Long.MAX_VALUE;
			ITrackedQueue<E> oldestQueue = null;
			for(Entry<String, ITrackedQueue<E>> partitionQueue : this.queue.getQueuesByGroup().entrySet()) {
				ITrackedQueue<E> queue = partitionQueue.getValue();
				if(queue.getOldestItemTime() < oldest) {
					oldest = queue.getOldestItemTime();
					oldestQueue = queue;
				}
			}
			return oldestQueue;
		}

        public void setPartitionedQueueue(IGroupedQueue<E> queue) {
            this.queue = queue;
        }
		
	}
	
	
	public static class RoundRobinPartition<E extends Groupable> implements GroupedRouter<E> {
		//private final AtomicReference lastPartition = new AtomicReference();
		private IGroupedQueue<E> queue;
		
		private RoundRobinRouter<String> router;
		
		
		public ITrackedQueue<E> nextPartition() {
		    String partition = router.next();
		    if(partition == null)
		        return null;
		    return queue.getQueueByGroup(partition);
		}	
		
		//TODO: it would be nice if peek really worked... but not necessary
		public ITrackedQueue<E> peekPartition() {
			return nextPartition();
		}

        public void setPartitionedQueueue(final IGroupedQueue<E> queue) {
            this.queue = queue;
            router = new RoundRobinRouter<String>(new Callable<List<String>>(){
                public List<String> call() throws Exception {
                    return queue.getGroups();
                }}, new RouteSkipAdapter<String>() {
                    public boolean shouldSkip(String item) {
                        return queue.getQueueByGroup(item).size() == 0;
                    }
                });
        }
	}
	
	public static class WeightedPartitionRouter<E extends Groupable> implements GroupedRouter<E> {
		public static interface PartitionWeigher<E extends Groupable> {
			public long getWeight(String partition, GroupedQueue<E> queue);
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

        public void setPartitionedQueueue(IGroupedQueue<E> queue) {
           
        }
		
	}
	
//	public static class WeightedFairRouter<E extends Partitionable & Weighted> implements PartitionRouter<E> {
//		public TrackedQueue<E> nextPartition(PartitionedQueue<E> queue) {
//			queue.get
//		}
//	}
}
