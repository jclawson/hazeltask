package com.succinctllc.hazelcast.work;


public interface WorkIdAdapter<W> {
	public WorkId getWorkId(W work);
}
