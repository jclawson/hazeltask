package com.hazeltask.executor;

import com.succinctllc.hazelcast.work.WorkId;

public interface WorkIdentifyable {
	public WorkId getWorkId();
}
