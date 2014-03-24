package com.hazeltask.executor.task;

import java.io.Serializable;




public interface TaskIdAdapter<T, GROUP extends Serializable, INFO extends Serializable> {
    
    /**
     * This might be useful to fill out to be able to get at task information
     * without having to deserialize the entire task. It is also the only
     * task-specific identifiable information available in TaskResponseListener
     * 
     * @param task
     * @return
     */
    public INFO getTaskInfo(T task);
    
    /**
     * The group does not need to be equal for successive calls with the same task
     * @param task
     * @return
     */
    public GROUP getTaskGroup(T task);
    
    /**
     * Given an object, return whether this adapater supports identifying the group
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
