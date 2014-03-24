package com.hazeltask.executor.task;

import java.io.Serializable;

import com.hazeltask.core.concurrent.collections.grouped.Groupable;


public class DefaultGroupableInfoProvidableTaskIdAdapter<I extends Serializable, G extends Serializable> implements TaskIdAdapter<TaskInfoProvidable<I,G>, G, Serializable> {

    @Override
    public G getTaskGroup(TaskInfoProvidable<I,G> task) {
        return task.getGroup();
    }

    @Override
    public boolean supports(Object task) {
       return task instanceof Groupable && task instanceof TaskInfoProvidable;
    }

    @Override
    public Serializable getTaskInfo(TaskInfoProvidable<I,G> task) {
        return task.getTaskInfo();
    }

}
