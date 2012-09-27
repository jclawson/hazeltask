package com.hazeltask.executor.task;



public interface WorkIdAdapter<T> {
	/**
	 * The WorkId returned does not need to be equal for 
	 * successive calls, but it should never duplicate an id
	 * for different works.  UUID.randomUUID is fine
	 * 
	 * @param task
	 * @return
	 */
    public WorkId createWorkId(T task);
}
