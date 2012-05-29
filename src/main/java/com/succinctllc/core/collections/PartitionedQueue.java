package com.succinctllc.core.collections;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.succinctllc.core.collections.PartitionedQueueRouter.PartitionRouter;
import com.succinctllc.core.collections.tracked.ITrackedQueue;
import com.succinctllc.core.collections.tracked.TrackedQueue;

public class PartitionedQueue<E extends Partitionable> extends AbstractQueue<E>  {
	private final ConcurrentMap<String, ITrackedQueue<E>> partitions;
	private final CopyOnWriteArrayList<String> partitionKeys = new CopyOnWriteArrayList<String>();
	private final PartitionRouter<E> partitionRouter;
	
	public static interface PartitionQueueListener<E> {
		public void onPartitionCreated(String partition, ITrackedQueue<E> q);
	}
	
	public PartitionedQueue(){
		partitions = createPartitionMap();
		partitionRouter = createPartitionRouter();
	}
	
	protected PartitionRouter<E> createPartitionRouter(){
		return new PartitionedQueueRouter.InOrderRouter<E>(this);
	}
	
	protected ConcurrentMap<String, ITrackedQueue<E>> getPartitionMap() {
		return this.partitions;
	}
	
	protected List<String> getPartitions() {
		return partitionKeys;
	}
	
	private ConcurrentMap<String, ITrackedQueue<E>> createPartitionMap() {
		return new ConcurrentHashMap<String, ITrackedQueue<E>>();
	}
	
	protected ITrackedQueue<E> createQueue(){
		return new TrackedQueue<E>();//ConcurrentLinkedQueue<E>();
	}
	
	private Queue<E> getOrCreatePartitionQueue(String partition) {
		Queue<E> q = partitions.get(partition);
		if(q == null) {
			ITrackedQueue<E> newQ = createQueue();
			if(partitions.putIfAbsent(partition, newQ) == null) {
				q = newQ;
				partitionKeys.add(partition);
			} else {
				q = partitions.get(partition);
			}
		}
		return q;
	}

	public boolean offer(E e) {
		String partition = e.getPartition();
		Queue<E> q = getOrCreatePartitionQueue(partition);
		return q.offer(e);
	}
	
	

	public E peek() {
		ITrackedQueue<E> oldestQueue = partitionRouter.peekPartition();
		if(oldestQueue != null)
			return oldestQueue.peek();
		else
			return null;
	}

	public E poll() {
		ITrackedQueue<E> oldestQueue = partitionRouter.nextPartition();
		if(oldestQueue != null)
			return oldestQueue.poll();
		else
			return null;
	}
	
	@Override
	public Iterator<E> iterator() {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public int size() {
		int size = 0;
		for(Queue<E> q : partitions.values()) {
			size += q.size();
		}
		return size;
	}

	public int drainTo(String partition, Collection<? super E> toCollection) {
		E elem = null;
		int num = 0;
		Queue<E> q = getPartitionMap().get(partition);
		while((elem = q.poll()) != null) {
			toCollection.add(elem);
			num++;
		}
		return num;
	}

	public int drainTo(String partition, Collection<? super E> toCollection, int max) {
		E elem = null;
		int num = 0;
		Queue<E> q = getPartitionMap().get(partition);
		while(num < max && (elem = q.poll()) != null) {
			toCollection.add(elem);
			num++;
		}
		return num;
	}	
}
