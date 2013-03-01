package com.hazeltask.executor.task;

import java.util.UUID;





public class DefaultTaskIdAdapter implements TaskIdAdapter<Object, String, String> {
    @Override
    public String getTaskId(Object task) {
        return UUID.randomUUID().toString();
    }

    @Override
    public String getTaskGroup(Object task) {
        return "$$DefaultGroup$$";
    }

    @Override
    public boolean supports(Object task) {
        //we support all objects!
        return true;
    }
}
