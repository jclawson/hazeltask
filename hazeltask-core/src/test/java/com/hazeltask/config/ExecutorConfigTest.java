package com.hazeltask.config;

import static org.junit.Assert.*;

import java.io.Serializable;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.hazeltask.executor.task.DefaultTaskIdAdapter;
import com.hazeltask.executor.task.TaskIdAdapter;

public class ExecutorConfigTest {
    
    ExecutorConfig config;
    
    @Before
    public void before() {
        config = new ExecutorConfig();
    }
    
    @Test
    public void disableWorkers() {
        assertFalse(config.isDisableWorkers());
        config.disableWorkers();
        assertTrue(config.isDisableWorkers());
        config.withDisableWorkers(false);
        assertFalse(config.isDisableWorkers());
    }
    
    @Test
    public void corePoolSize() {
        assertEquals(4, config.getThreadCount());
        config.withThreadCount(10);
        assertEquals(10, config.getThreadCount());
    }
    
//    @Test
//    public void maxPoolSize() {

//    }
    
    @Test
    public void fixedThreadPoolTest_1() {
        assertEquals(4, config.getThreadCount());
        assertEquals(4, config.getMaxThreadPoolSize());
        config.withThreadCount(10);
        assertEquals(10, config.getThreadCount());
        assertEquals(10, config.getMaxThreadPoolSize());
    }
    
    @Test
    public void maxThreadKeepAlive() {
        assertEquals(60000, config.getMaxThreadKeepAliveTime());
        config.withMaxThreadKeepAliveTime(1);
        assertEquals(1, config.getMaxThreadKeepAliveTime());
    }
    
    @Test
    public void taskIdAdapter() {
        TaskIdAdapter tmp = new TaskIdAdapter() {
            public Serializable getTaskGroup(Object task) {return null;}

            public boolean supports(Object task) {return false;}

            @Override
            public Serializable getTaskInfo(Object task) {
                return null;
            }
        };
        assertEquals(DefaultTaskIdAdapter.class, config.getTaskIdAdapter().getClass());
        config.withTaskIdAdapter(tmp);
        assertEquals(tmp, config.getTaskIdAdapter());
    }
    
    @Test
    public void autoStart() {
        assertEquals(true, config.isAutoStart());
        config.disableAutoStart();
        assertEquals(false, config.isAutoStart());
        config.withAutoStart(true);
        assertEquals(true, config.isAutoStart());
    }
    
    @Test
    public void enableFutureTracking() {
        assertEquals(true, config.isFutureSupportEnabled());
        config.disableFutureSupport();
        assertEquals(false, config.isFutureSupportEnabled());
    }
    
    @Test
    public void maximumFutureWaitTime() {
        assertEquals(3600000, config.getMaximumFutureWaitTime());
        config.withMaximumFutureWaitTime(1);
        assertEquals(1, config.getMaximumFutureWaitTime());
    }
    
    @Test
    public void asyncronousTaskDistribution() {
        assertFalse(config.isAsyncronousTaskDistribution());
        config.useAsyncronousTaskDistribution();
        assertTrue(config.isAsyncronousTaskDistribution());
    }
    
    @Test
    public void asyncronousTaskDistributionQueueSize() {
        assertEquals(500, config.getAsyncronousTaskDistributionQueueSize());
        config.withAsyncronousTaskDistributionQueueSize(1);
        assertEquals(1, config.getAsyncronousTaskDistributionQueueSize());
    }
    
    @Test
    public void recoveryProcessPollInterval() {
        assertEquals(30000, config.getRecoveryProcessPollInterval());
        config.withRecoveryProcessPollInterval(1);
        assertEquals(1, config.getRecoveryProcessPollInterval());
    }
    
    @Test
    public void executorLoadBalancingConfig() {
        assertNotNull(config.getLoadBalancingConfig());
        config.withLoadBalancingConfig(null);
        assertNull(config.getLoadBalancingConfig());
    }
    
    
    @Test
    public void testDefaultConfig() {
        ExecutorConfig defaultConfig = new ExecutorConfig();
        TaskIdAdapter adapter = defaultConfig.getTaskIdAdapter();
        
        
        Object task = new Object();
        Assert.assertTrue(adapter.supports(task));
        
        Object group = adapter.getTaskGroup(task);
        
        Assert.assertEquals(Integer.class, group.getClass());
        Assert.assertEquals(Integer.MIN_VALUE, (int)(Integer)group);
    }
}
