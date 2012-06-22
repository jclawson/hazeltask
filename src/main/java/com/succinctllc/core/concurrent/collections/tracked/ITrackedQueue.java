package com.succinctllc.core.concurrent.collections.tracked;

import java.util.Queue;

public interface ITrackedQueue<E> extends Queue<E> {
    public Long getOldestItemTime();
    public Long getLastAddedTime();
    public Long getLastRemovedTime();
}
