package com.hazeltask.core.concurrent.collections.grouped;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazeltask.core.concurrent.collections.router.ListRouter;
import com.hazeltask.core.concurrent.collections.router.ListRouterFactory;
import com.hazeltask.core.concurrent.collections.router.RouteSkipAdapter;
import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;

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
	
	
	public static class GroupRouterAdapter<E extends Groupable> implements GroupedRouter<E> {
	    ILogger logger = Logger.getLogger(GroupRouterAdapter.class.getName());
		private IGroupedQueue<E> queue;
		private ListRouter<String> router;		
		private final ListRouterFactory<String> routerFactory;
		
		public GroupRouterAdapter(ListRouterFactory<String> routerFactory) {
		    this.routerFactory = routerFactory;
		}
		
		private ITrackedQueue<E> nextPartition(int tryNumber) {
		    String partition = router.next();
            if(partition == null)
                return null;
            ITrackedQueue<E> q = queue.getQueueByGroup(partition);
            if(q.size() == 0) {
                if(tryNumber >= 10) {
                    return null;
                }
                q = nextPartition(tryNumber + 1);
            }
            
            return q;
		}
		
		public ITrackedQueue<E> nextPartition() {
		    return nextPartition(0);
		}	
		
		public ITrackedQueue<E> peekPartition() {
			return nextPartition();
		}

        public void setPartitionedQueueue(final IGroupedQueue<E> queue) {
            this.queue = queue;
            this.router = routerFactory.createRouter(new Callable<List<String>>(){
                public List<String> call() throws Exception {
                    return queue.getNonEmptyGroups();
                }});
        }
	}
	
//	public static class WeightedPartitionRouter<E extends Groupable> implements GroupedRouter<E> {
//		public static interface PartitionWeigher<E extends Groupable> {
//			public long getWeight(String partition, IGroupedQueue<E> queue);
//		}
//		
//		private final PartitionWeigher<E> weigher;
//		public WeightedPartitionRouter(PartitionWeigher<E> weigher){
//			this.weigher = weigher;
//		}
//		
//		public ITrackedQueue<E> nextPartition() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//		
//		public ITrackedQueue<E> peekPartition() {
//			return nextPartition();
//		}
//
//        public void setPartitionedQueueue(IGroupedQueue<E> queue) {
//           
//        }
//		
//	}
	
//	public static class WeightedFairRouter<E extends Partitionable & Weighted> implements PartitionRouter<E> {
//		public TrackedQueue<E> nextPartition(PartitionedQueue<E> queue) {
//			queue.get
//		}
//	}
}
