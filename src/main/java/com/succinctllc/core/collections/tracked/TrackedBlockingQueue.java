package com.succinctllc.core.collections.tracked;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class TrackedBlockingQueue<E> extends AbstractQueue<E> implements ITrackedQueue<E>  {
	
	private final BlockingDeque<TrackedItem<E>> delegate;
	
	public TrackedBlockingQueue(){
		delegate = createQueue();
	}
	
	protected BlockingDeque<TrackedItem<E>> createQueue() {
		return new LinkedBlockingDeque<TrackedItem<E>>();
	}
	
	private LinkedBlockingDeque<TrackedItem<E>> getDelegate(){
		return (LinkedBlockingDeque<TrackedItem<E>>) this.delegate;
	}
	
	public long getNewestTime(){
		return getDelegate().peekLast().getTimestamp();
	}
	
	private TrackedItem<E> wrap(E e){
		return new TrackedItem<E>(e);
	}

	public boolean offer(E e) {
		return delegate.offer(wrap(e));
	}

	public E peek() {
		return delegate.peek().getEntry();
	}

	public E poll() {
		return delegate.poll().getEntry();
	}

	public long getOldestTime() {
		return delegate.peekFirst().getTimestamp();
	}

	@Override
	public Iterator<E> iterator() {
		final Iterator<TrackedItem<E>> itDelegate = delegate.iterator();
		return new Iterator<E>(){
			public boolean hasNext() {
				return itDelegate.hasNext();
			}

			public E next() {
				TrackedItem<E> item = itDelegate.next();
				if(item != null)
					return item.getEntry();
				else
					return null;
			}

			public void remove() {
				itDelegate.remove();
			}
		};
	}

	@Override
	public int size() {
		return delegate.size();
	}
	
	
	
}
