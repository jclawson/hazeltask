package com.hazeltask.executor.task;

import java.io.Serializable;






public class DefaultTaskIdAdapter implements TaskIdAdapter<Object, Integer, Serializable> {
    private static int GROUP = Integer.MIN_VALUE;

    @Override
    public Integer getTaskGroup(Object task) {
        return GROUP;
    }

    @Override
    public boolean supports(Object task) {
        //we support all objects!
        return true;
    }

    @Override
    public Serializable getTaskInfo(Object task) {
        return null;
    }
}
