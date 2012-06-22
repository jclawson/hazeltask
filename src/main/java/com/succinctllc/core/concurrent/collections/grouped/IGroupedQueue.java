package com.succinctllc.core.concurrent.collections.grouped;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.succinctllc.core.concurrent.collections.tracked.ITrackedQueue;
import com.succinctllc.core.concurrent.collections.tracked.TrackedQueue;


public interface IGroupedQueue<E extends Groupable> {

    public abstract Long getOldestQueueTime();

    public abstract int drainTo(String partition, Collection<? super E> toCollection);

    public abstract int drainTo(String partition, Collection<? super E> toCollection, int max);

    public List<String> getGroups();
    
    public ITrackedQueue<E> getQueueByGroup(String group);
    
    public Map<String, ITrackedQueue<E>> getQueuesByGroup();
    
}