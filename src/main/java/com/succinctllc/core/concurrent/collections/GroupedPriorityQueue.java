package com.succinctllc.core.concurrent.collections;

import java.util.AbstractQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.succinctllc.core.concurrent.collections.grouped.Groupable;
import com.succinctllc.core.concurrent.collections.grouped.GroupedQueueRouter;
import com.succinctllc.core.concurrent.collections.grouped.TrackedQueue;
import com.succinctllc.core.concurrent.collections.grouped.GroupedQueueRouter.GroupedRouter;

public class GroupedPriorityQueue<E extends Groupable> extends AbstractQueue<E> {
    private final ConcurrentMap<String, TrackedQueue<E>> queuesByGroup;
    private final CopyOnWriteArrayList<String> groups = new CopyOnWriteArrayList<String>();
    private final GroupedRouter<E> groupRouter;
    
    public GroupedPriorityQueue(ListRouter<E> partitionRouter){
        queuesByGroup = new ConcurrentHashMap<String, TrackedQueue<E>>();
        this.groupRouter = partitionRouter == null 
                                    ? new GroupedQueueRouter.InOrderRouter<E>() 
                                    : partitionRouter;
        this.groupRouter.setPartitionedQueueue(this);
    }
}
