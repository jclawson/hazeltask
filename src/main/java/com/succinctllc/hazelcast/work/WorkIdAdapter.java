package com.succinctllc.hazelcast.work;


public interface WorkIdAdapter<W> {
	/**
	 * The WorkId returned does not need to be equal for 
	 * successive calls.
	 * 
	 * @param work
	 * @return
	 */
    public WorkId createWorkId(W work);
}
