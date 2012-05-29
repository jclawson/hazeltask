package com.succinctllc.core.collections.tracked;

import java.util.Queue;

public interface ITrackedQueue<E> extends Queue<E> {

	public abstract long getNewestTime();

	public abstract long getOldestTime();

}