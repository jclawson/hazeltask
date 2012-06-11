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
	 * 
	 * FIXME: this needs to be an atomic long becaues we have
	 * multiple threads accessing it...  or lets just not keep 
	 * track of it... do we even use it???
	 */
	//private volatile long newestTimestamp;
	
	//private volatile boolean inSync = true;
	private QueueListener<TrackedItem<E>> listener;
	
	
	public TrackedQueue(){
		delegate = createQueue();
	}
	
	public TrackedQueue(QueueListener<TrackedItem<E>> listener) {
	    this();
	    this.listener = listener;
	    this.listener.setQueue(delegate);
	}
	
	private TrackedItem<E> wrap(E e){
		return new TrackedItem<E>(e);
	}
	
	public boolean offer(E e) {
		//newestTimestamp = System.currentTimeMillis();
	    TrackedItem<E> wrappedElem = wrap(e);
	    if(delegate.offer(wrappedElem)) {
	        if(listener != null)
	            listener.onAdd(wrappedElem);
	        return true;
	    }
	    return false;
	}

	public E peek() {
	    TrackedItem<E> tItem = delegate.peek();
	    if(tItem == null)
            return null;
	    return tItem.getEntry();
	}

	public E poll() {
		TrackedItem<E> tItem = delegate.poll();
		if(tItem == null)
		    return null;
		
	    E result = tItem.getEntry();
		onRemove(tItem);
		return result;
	}
	
	private void onRemove(TrackedItem<E> tItem){
		//if(this.size() == 0)
		//	this.newestTimestamp = 0;
	    if(listener != null)
	        listener.onRemove(tItem);
	}

	@Override
	public Iterator<E> iterator() {
		return new TrackedQueueIterator<E>(this);
	}

	@Override
	public int size() {
		return delegate.size();
	}
	
//	public long getNewestTime(){
//		if(!inSync) {
//			syncNewestTime();
//		}
//		return newestTimestamp;
//	}
	
	private void syncNewestTime(){
//		Iterator<TrackedItem<E>> it = delegate.iterator();
//		TrackedItem<E> elem = null;
//		while(it.hasNext()) {
//			elem = it.next();
//		}				
//		if(elem != null)
//			newestTimestamp = elem.getTimestamp();
//		inSync = true;
	}
	
	public long getOldestTime(){
		TrackedItem<E> entry = delegate.peek();
		if(entry == null)
			return Long.MAX_VALUE;
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
//			long timestamp = lastEntry.getTimestamp();
//			if(trackedQueue.getNewestTime() == timestamp) {
//				//we removed the last entry we added...
//				//the newestTime is now out of sync!
//				trackedQueue.inSync = false;
//			}
		    trackedQueue.onRemove(lastEntry);
			delegate.remove();
		}
		
	}
	
	
	protected Queue<TrackedItem<E>> createQueue(){
		return new ConcurrentLinkedQueue<TrackedItem<E>>();
	}

	
}
