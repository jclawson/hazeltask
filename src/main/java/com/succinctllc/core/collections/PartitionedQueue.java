package com.succinctllc.core.collections;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
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
	
	public PartitionedQueue(PartitionRouter<E> partitionRouter){
		partitions = createPartitionMap();
		this.partitionRouter = partitionRouter == null 
		                            ? new PartitionedQueueRouter.InOrderRouter<E>() 
		                            : partitionRouter;
		this.partitionRouter.setPartitionedQueueue(this);
	}
	
	public PartitionedQueue(){
        this(null);
    }
	
	protected ConcurrentMap<String, ITrackedQueue<E>> getPartitionMap() {
		return this.partitions;
	}
	
	protected ITrackedQueue<E> getPartition(String partition){
	    return this.partitions.get(partition);
	}
	
	protected List<String> getPartitions() {
		return partitionKeys;
	}
	
	private ConcurrentMap<String, ITrackedQueue<E>> createPartitionMap() {
		return new ConcurrentHashMap<String, ITrackedQueue<E>>();
	}
	
	protected ITrackedQueue<E> createQueue(){
		return new TrackedQueue<E>();
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
		boolean result = q.offer(e);
		notify();
		return result;
	}
	
	
	//FIXME: guarantee that it will return non-null if queue is not empty (up to router?)
	public E peek() {
		ITrackedQueue<E> oldestQueue = partitionRouter.peekPartition();
		if(oldestQueue != null)
			return oldestQueue.peek();
		else
			return null;
	}

	//FIXME: guarantee that it will return non-null if queue is not empty (up to router?)
	public E poll() {
		ITrackedQueue<E> oldestQueue = partitionRouter.nextPartition();
		if(oldestQueue != null)
			return oldestQueue.poll();
		else
			return null;
	}
	

	/**
	 * This iterator does not guarantee it will iterate in insertion order.  It will
	 * simply iterate each partition queue one by one
	 * @return
	 */
	@Override
	public Iterator<E> iterator() {
		return new FastPartitionedQueueIterator();
	}
	
	public class FastPartitionedQueueIterator implements Iterator<E> {
		private final List<Iterator<E>> queueIterators; 
		private int currentQueue = 0;
		
		public FastPartitionedQueueIterator(){
			queueIterators = new ArrayList<Iterator<E>>(partitions.size());
			for(ITrackedQueue<E> q : partitions.values()) {
				queueIterators.add(q.iterator());
			}
		}
		
		private Iterator<E> getIterator(){
			if(currentQueue >= queueIterators.size()) {
				return null;
			}
			
			Iterator<E> it = queueIterators.get(currentQueue);	
			
			do {
				if(it.hasNext()) {
					return it;
				}
				currentQueue++;
				if(currentQueue >= queueIterators.size()) {
				    return null;
				}
				
				it = queueIterators.get(currentQueue);
			} while(currentQueue < queueIterators.size());
			
			if(it.hasNext()) {
				return it;
			} else {
				return null;
			}
		}
		
		public boolean hasNext() {
			Iterator<E> it = getIterator();
			if(it != null)
				return true;
			else
				return false;
		}

		public E next() {
			Iterator<E> it = getIterator();
			if(it != null)
				return it.next();
			else
				return null;
		}

		public void remove() {
			Iterator<E> it = queueIterators.get(currentQueue);
			if(it != null)
				it.remove();
		}
		
	}
	
	public long getOldestQueueTime() {
	    long oldestTime = Long.MAX_VALUE;
	    for(Entry<String, ITrackedQueue<E>> entry : this.partitions.entrySet()) {
	        long oldest = entry.getValue().getOldestTime();
	        if(oldest < oldestTime)
	            oldestTime = oldest;
	    }
	    
	    return oldestTime;
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
