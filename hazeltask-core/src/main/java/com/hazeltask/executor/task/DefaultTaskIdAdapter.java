package com.hazeltask.executor.task;

import java.util.UUID;





public class DefaultTaskIdAdapter implements TaskIdAdapter<Object, Integer> {
    private static int GROUP = Integer.MIN_VALUE;
    
    @Override
    public UUID getTaskId(Object task) {
        return UUID.randomUUID();
    }

    @Override
    public Integer getTaskGroup(Object task) {
        return GROUP;
    }

    @Override
    public boolean supports(Object task) {
        //we support all objects!
        return true;
    }
}
