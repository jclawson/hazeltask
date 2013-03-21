package com.hazeltask.executor.task;






public class DefaultTaskIdAdapter implements TaskIdAdapter<Object, Integer> {
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
}
