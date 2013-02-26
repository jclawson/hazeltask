package com.hazeltask.batch;

import java.io.Serializable;
import java.util.Collection;

import com.hazeltask.executor.task.Task;

public interface TaskBatch<I, ID extends Serializable, GROUP extends Serializable> extends Task<ID, GROUP> {
    public Collection<I> getItems();
}
