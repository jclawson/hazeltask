package com.hazeltask.executor.task;

import java.io.Serializable;
import java.util.UUID;

import com.hazeltask.core.concurrent.collections.grouped.Groupable;

public class DefaultGroupableTaskIdAdapter<G extends Serializable> implements TaskIdAdapter<Groupable<G>, UUID, G> {

    @Override
    public UUID getTaskId(Groupable<G> task) {
        return UUID.randomUUID();
    }

    @Override
    public G getTaskGroup(Groupable<G> task) {
        return task.getGroup();
    }

    @Override
    public boolean supports(Object task) {
       return task instanceof Groupable;
    }

}
