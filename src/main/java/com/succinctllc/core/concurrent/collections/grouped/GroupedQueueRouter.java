package com.succinctllc.core.concurrent.collections.grouped;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import com.succinctllc.hazelcast.work.router.RoundRobinRouter;
import com.succinctllc.hazelcast.work.router.RouteSkipAdapter;

public class GroupedQueueRouter {
	public static interface GroupedRouter<E extends Groupable> {
		public TrackedQueue<E> nextPartition();
		public TrackedQueue<E> peekPartition();
		public void setPartitionedQueueue(GroupedQueue<E> queue);
	}
	
	public static class InOrderRouter<E extends Groupable> implements GroupedRouter<E> {
		private GroupedQueue<E> queue;
		
		public TrackedQueue<E> peekPartition() {
			return nextPartition();
		}
		
		public TrackedQueue<E> nextPartition() {
			return getOldestQueue();
		}
		
		protected TrackedQueue<E> getOldestQueue(){
			long oldest = Long.MAX_VALUE;
			TrackedQueue<E> oldestQueue = null;
			for(Entry<String, TrackedQueue<E>> partitionQueue : this.queue.getQueuesByGroup().entrySet()) {
				TrackedQueue<E> queue = partitionQueue.getValue();
				if(queue.getOldestTime() < oldest) {
					oldest = queue.getOldestTime();
					oldestQueue = queue;
				}
			}
			return oldestQueue;
		}

        public void setPartitionedQueueue(GroupedQueue<E> queue) {
            this.queue = queue;
        }
		
	}
	
	
	public static class RoundRobinPartition<E extends Groupable> implements GroupedRouter<E> {
		//private final AtomicReference lastPartition = new AtomicReference();
		private GroupedQueue<E> queue;
		
		private RoundRobinRouter<String> router;
		
		
		public TrackedQueue<E> nextPartition() {
		    String partition = router.next();
		    if(partition == null)
		        return null;
		    return queue.getQueueByGroup(partition);
		}	
		
		//TODO: it would be nice if peek really worked... but not necessary
		public TrackedQueue<E> peekPartition() {
			return nextPartition();
		}

        public void setPartitionedQueueue(final GroupedQueue<E> queue) {
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
		
		public TrackedQueue<E> nextPartition() {
			// TODO Auto-generated method stub
			return null;
		}
		
		public TrackedQueue<E> peekPartition() {
			return nextPartition();
		}

        public void setPartitionedQueueue(GroupedQueue<E> queue) {
           
        }
		
	}
	
//	public static class WeightedFairRouter<E extends Partitionable & Weighted> implements PartitionRouter<E> {
//		public TrackedQueue<E> nextPartition(PartitionedQueue<E> queue) {
//			queue.get
//		}
//	}
}
