package com.hazeltask.executor.task;



public class DefaultTaskIdAdapter implements TaskIdAdapter<Object> {
    public TaskId createTaskId(Object task) {
        return new TaskId("$$DefaultGroup$$");
    }
}
