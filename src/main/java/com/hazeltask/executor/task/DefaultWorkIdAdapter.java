package com.hazeltask.executor.task;



public class DefaultWorkIdAdapter implements WorkIdAdapter<Object> {
    public WorkId createWorkId(Object task) {
        return new WorkId("$$DefaultGroup$$");
    }
}
