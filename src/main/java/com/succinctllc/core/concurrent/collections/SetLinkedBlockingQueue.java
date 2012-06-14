package com.succinctllc.core.concurrent.collections;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.hazelcast.util.ConcurrentHashSet;

/**
 * This LinkedBlockingQueue prevents duplicate items from being submitted into the queue.
 * 
 * @author Jason Clawson
 *
 * @param <E>
 */
public class SetLinkedBlockingQueue<E> extends LinkedBlockingQueue<E> {
	private static final long serialVersionUID = 1L;
	
	private ConcurrentHashSet<E> items = new ConcurrentHashSet<E>();

	@Override
	public void put(E e) throws InterruptedException {
		if(items.add(e))
			super.put(e);
	}

	@Override
	public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
		boolean result = false;
		if(items.add(e)) {
			try {
				result = super.offer(e, timeout, unit);
			} finally {
				if(!result)
					items.remove(e); //this is a race condition but thats ok
			}
		}
		return result;
	}

	@Override
	public boolean offer(E e) {
		boolean result = false;
		if(items.add(e)) {
			result = super.offer(e);
			if(!result)
				items.remove(e); //this is a race condition but thats ok
		}
		
		return result;
	}

	@Override
	public E take() throws InterruptedException {
		E el = super.take();
		items.remove(el);
		return el;
	}

	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		E el = super.poll(timeout, unit);
		if(el != null)
			items.remove(el);
		return el;
	}

	@Override
	public E poll() {
		E el = super.poll();
		if(el != null)
			items.remove(el);
		return el;
	}

	@Override
	public boolean remove(Object o) {
		boolean result = super.remove(o);
		if(result)
			items.remove(o);
		return result;
	}

	@Override
	public void clear() {
		items.clear();
		super.clear();
	}

	@Override
	public int drainTo(Collection<? super E> c) {
		int num = super.drainTo(c);
		if(num > 0)
			items.removeAll(c);
		return num;
	}

	@Override
	public int drainTo(Collection<? super E> c, int maxElements) {
		int num =  super.drainTo(c, maxElements);
		if(num > 0)
			items.removeAll(c);
		return num;
	}

	
	
}
