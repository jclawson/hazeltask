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
    
    /**
     * Given an object, return whether this adapater supports identifying the id and group
     * Generally the logic is: <code>return task instanceof T</code>
     * 
     * We have this here as a validation step when you submit a Runnable to the ExecutorService.
     * Since ExecutorService isn't generic, we need to take extra steps to ensure the task
     * is able to be processed by the system
     * 
     * @param task
     * @return
     */
    public boolean supports(Object task);
}
