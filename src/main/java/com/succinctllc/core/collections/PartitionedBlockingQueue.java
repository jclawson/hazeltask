package com.succinctllc.core.collections;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.succinctllc.core.collections.tracked.ITrackedQueue;
import com.succinctllc.core.collections.tracked.TrackedBlockingQueue;


public class PartitionedBlockingQueue<E extends Partitionable> extends PartitionedQueue<E> implements BlockingQueue<E>  {

	
	@Override
	protected ITrackedQueue<E> createQueue() {
		return new TrackedBlockingQueue<E>();
	}

	public int drainTo(Collection<? super E> c) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int drainTo(Collection<? super E> c, int maxElements) {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean offer(E e, long timeout, TimeUnit unit)
			throws InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	public void put(E e) throws InterruptedException {
		// TODO Auto-generated method stub
		
	}

	public int remainingCapacity() {
		// TODO Auto-generated method stub
		return 0;
	}

	public E take() throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}
	
}
