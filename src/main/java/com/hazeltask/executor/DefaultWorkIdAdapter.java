package com.hazeltask.executor;

import java.util.concurrent.Callable;

import com.succinctllc.hazelcast.work.WorkId;

public class DefaultWorkIdAdapter implements WorkIdAdapter {
    public WorkId createWorkId(Callable<?> task) {
        return new WorkId("$$DefaultGroup$$");
    }
}
