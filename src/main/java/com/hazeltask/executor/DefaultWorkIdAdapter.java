package com.hazeltask.executor;


public class DefaultWorkIdAdapter implements WorkIdAdapter<Object> {
    public WorkId createWorkId(Object task) {
        return new WorkId("$$DefaultGroup$$");
    }
}
