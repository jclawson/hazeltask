package com.hazeltask.batch;

import java.util.UUID;

public class DefaultBatchKeyAdapter<I> extends BatchKeyAdapter<I, TaskBatch<I, String, String>, String, String, String> {
    @Override
    public String getItemGroup(I o) {
        return "$$DefaultGroup$$";
    }

    @Override
    public String getItemId(I o) {
        return UUID.randomUUID().toString();
    }

    @Override
    public boolean isConsistent() {
        return false;
    }

    @Override
    public String getTaskId(TaskBatch<I, String, String> task) {
        return task.getId();
    }

    @Override
    public String getTaskGroup(TaskBatch<I, String, String> task) {
        return task.getGroup();
    }

    @Override
    public boolean supports(Object task) {
        return true;
    }

}
