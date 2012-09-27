package com.hazeltask.batch;

import java.util.Collection;

import com.hazeltask.executor.task.Task;

public interface TaskBatch<I> extends Task {
    public Collection<I> getItems();
}
