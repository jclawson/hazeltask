package com.succinctllc.core.collections.tracked;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TrackedQueue<E> extends AbstractQueue<E> implements ITrackedQueue<E> {
	
	protected final Queue<TrackedItem<E>> delegate;
	
	/**
	 * This must be volatile so that reads are always reading the 
	 * correct data.  This is higher performing than an AtomicLong
	 */
	private volatile long newestTimestamp;
	
	private volatile boolean inSync = true;
	
	public TrackedQueue(){
		delegate = createQueue();
	}
	
	private TrackedItem<E> wrap(E e){
		return new TrackedItem<E>(e);
	}
	
	public boolean offer(E e) {
		newestTimestamp = System.currentTimeMillis();
		return delegate.offer(wrap(e));
	}

	public E peek() {
		return delegate.peek().getEntry();
	}

	public E poll() {
		E result = delegate.poll().getEntry();
		onRemove();
		return result;
	}
	
	private void onRemove(){
		if(this.size() == 0)
			this.newestTimestamp = 0;
	}

	@Override
	public Iterator<E> iterator() {
		return new TrackedQueueIterator<E>(this);
	}

	@Override
	public int size() {
		return delegate.size();
	}
	
	/* (non-Javadoc)
	 * @see com.succinctllc.core.collections.ITrackedQueue#getNewestTime()
	 */
	public long getNewestTime(){
		if(!inSync) {
			syncNewestTime();
		}
		return newestTimestamp;
	}
	
	private void syncNewestTime(){
		Iterator<TrackedItem<E>> it = delegate.iterator();
		TrackedItem<E> elem = null;
		while(it.hasNext()) {
			elem = it.next();
		}				
		if(elem != null)
			newestTimestamp = elem.getTimestamp();
		inSync = true;
	}
	
	/* (non-Javadoc)
	 * @see com.succinctllc.core.collections.ITrackedQueue#getOldestTime()
	 */
	public long getOldestTime(){
		TrackedItem<E> entry = delegate.peek();
		if(entry == null)
			return 0;
		return entry.getTimestamp();
	}
	
	public static class TrackedQueueIterator<I> implements Iterator<I> {
		
		private final TrackedQueue<I> trackedQueue;
		private final Iterator<TrackedItem<I>> delegate;
		private TrackedItem<I> lastEntry;
		
		public TrackedQueueIterator(TrackedQueue<I> trackedQueue){
			this.trackedQueue = trackedQueue;
			this.delegate = trackedQueue.delegate.iterator();
		}
		
		public boolean hasNext() {
			return delegate.hasNext();
		}

		public I next() {
			lastEntry = delegate.next();
			return lastEntry.getEntry();
		}

		public void remove() {
			long timestamp = lastEntry.getTimestamp();
			if(trackedQueue.getNewestTime() == timestamp) {
				//we removed the last entry we added...
				//the newestTime is now out of sync!
				trackedQueue.inSync = false;
			}
			
			delegate.remove();
		}
		
	}
	
	
	protected Queue<TrackedItem<E>> createQueue(){
		return new ConcurrentLinkedQueue<TrackedItem<E>>();
	}

	
}
