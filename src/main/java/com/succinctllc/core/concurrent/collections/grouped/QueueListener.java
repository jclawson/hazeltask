package com.succinctllc.core.concurrent.collections.grouped;

import java.util.Queue;

/**
 * Used to keep track of custom stats on a TrackedQueue
 * 
 * You may choose to modify the stats object that is passed in
 * but you must always return a stats object representing the new
 * state
 * 
 * This is executed on the thread calling the add (which there may be many)
 * so your code must be reentrant, and must be fast
 * 
 * @author jclawson
 *
 * @param <E>
 */
public interface QueueListener<E> {
    public void onAdd(E item);
    public void onRemove(E item);
    public void setQueue(Queue<E> q);
}
