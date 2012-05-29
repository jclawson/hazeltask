package com.succinctllc.core.collections;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import com.succinctllc.core.collections.tracked.ITrackedQueue;

public class PartitionedQueueRouter {
	public static interface PartitionRouter<E extends Partitionable> {
		public ITrackedQueue<E> nextPartition();
		public ITrackedQueue<E> peekPartition();
	}
	
	public static class InOrderRouter<E extends Partitionable> implements PartitionRouter<E> {
		private final PartitionedQueue<E> queue;
		public InOrderRouter(PartitionedQueue<E> queue){
			this.queue = queue;
		}
		
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
		
	}
	
	public static class RoundRobinPartition<E extends Partitionable> implements PartitionRouter<E> {
		private final AtomicInteger lastIndex = new AtomicInteger(0);
		private final PartitionedQueue<E> queue;
		
		public RoundRobinPartition(PartitionedQueue<E> queue){
			this.queue = queue;
		}
		
		public ITrackedQueue<E> nextPartition() {
			List<String> partitions = queue.getPartitions();
			int size = partitions.size();
			if(size > 0) {
				lastIndex.compareAndSet(size, 0);
				int index = lastIndex.getAndIncrement();
				if(index >= size)
					index = 0;
				String partition = queue.getPartitions().get(index);
				queue.getPartitionMap().get(partition);
			}
			return null;
		}	
		
		public ITrackedQueue<E> peekPartition() {
			return queue.getPartitionMap().get(queue.getPartitions().get(lastIndex.get()));
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
		
	}
	
//	public static class WeightedFairRouter<E extends Partitionable & Weighted> implements PartitionRouter<E> {
//		public TrackedQueue<E> nextPartition(PartitionedQueue<E> queue) {
//			queue.get
//		}
//	}
}
