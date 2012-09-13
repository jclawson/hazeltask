package com.hazeltask.executor;

import com.succinctllc.hazelcast.work.WorkId;

public class DefaultWorkIdAdapter implements WorkIdAdapter<Object> {
    public WorkId createWorkId(Object task) {
        return new WorkId("$$DefaultGroup$$");
    }
}
