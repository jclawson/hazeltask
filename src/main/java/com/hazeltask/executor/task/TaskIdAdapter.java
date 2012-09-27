package com.hazeltask.executor.task;



public interface TaskIdAdapter<T> {
	/**
	 * The WorkId returned does not need to be equal for 
	 * successive calls, but it should never duplicate an id
	 * for different works.  UUID.randomUUID is fine
	 * 
	 * @param task
	 * @return
	 */
    public TaskId createTaskId(T task);
}
