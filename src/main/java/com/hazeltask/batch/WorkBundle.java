package com.hazeltask.batch;

import java.util.Collection;

import com.hazeltask.executor.task.Work;

public interface WorkBundle<I> extends Work {
    public Collection<I> getItems();
}
