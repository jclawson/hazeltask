package com.hazeltask.executor.task;

import java.io.Serializable;




public interface TaskIdAdapter<T, ID extends Serializable, GROUP extends Serializable> {
	/**
	 * The id returned does not need to be equal for 
	 * successive calls, but it should never duplicate an id
	 * for different works.  UUID.randomUUID is fine
	 * 
	 * @param task
	 * @return
	 */
    public ID getTaskId(T task);
    
    /**
     * The group does not need to be equal for successive calls with the same task
     * @param task
     * @return
     */
    public GROUP getTaskGroup(T task);
}
