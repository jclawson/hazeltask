package com.hazeltask.config;

import java.util.UUID;

import junit.framework.Assert;

import org.junit.Test;

import com.hazeltask.executor.task.TaskIdAdapter;

public class ExecutorConfigTest {
    
    @Test
    public void testDefaultConfig() {
        ExecutorConfig defaultConfig = new ExecutorConfig();
        TaskIdAdapter adapter = defaultConfig.getTaskIdAdapter();
        
        
        Object task = new Object();
        Assert.assertTrue(adapter.supports(task));
        
        Object group = adapter.getTaskGroup(task);
        Object id = adapter.getTaskId(task);
        
        Assert.assertEquals(Integer.class, group.getClass());
        Assert.assertEquals(Integer.MIN_VALUE, (int)(Integer)group);
        
        Assert.assertEquals(UUID.class, id.getClass());
    }
}
