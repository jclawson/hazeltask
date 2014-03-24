package com.hazeltask.executor.task;

import com.hazeltask.core.concurrent.collections.grouped.Groupable;


public interface TaskInfoProvidable<I, G> extends Groupable<G> {
    public I getTaskInfo();
}
