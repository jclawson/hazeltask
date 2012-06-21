package com.succinctllc.core.concurrent.collections;

import java.util.Queue;

public interface ITrackedQueue<E> extends Queue<E> {
    public long getOldestItemTime();
    public long getLastAddedTime();
    public long getLastRemovedTime();
}
