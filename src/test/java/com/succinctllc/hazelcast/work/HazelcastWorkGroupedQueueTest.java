package com.succinctllc.hazelcast.work;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

public class HazelcastWorkGroupedQueueTest {
    
    private HazelcastWorkGroupedQueue taskQueue;
    
    @Before
    public void setupData() {
        taskQueue = new HazelcastWorkGroupedQueue();
        
        HazelcastWork work1 = mock(HazelcastWork.class);
        HazelcastWork work2 = mock(HazelcastWork.class);
        HazelcastWork work3 = mock(HazelcastWork.class);
        HazelcastWork work4 = mock(HazelcastWork.class);
        
        when(work1.getGroup()).thenReturn("1");
        when(work2.getGroup()).thenReturn("1");
        when(work3.getGroup()).thenReturn("1");
        when(work4.getGroup()).thenReturn("1");
        
        when(work1.getTimeCreated()).thenReturn(1L);
        when(work2.getTimeCreated()).thenReturn(2L);
        when(work3.getTimeCreated()).thenReturn(3L);
        when(work4.getTimeCreated()).thenReturn(4L);
        
        taskQueue.add(work3);
        taskQueue.add(work1);
        taskQueue.add(work4);
        taskQueue.add(work2);
        
    }
    
    
    @Test
    public void testOldestWorkCreatedTime() {
        Assert.assertEquals(1, taskQueue.getOldestWorkCreatedTime());
    }
    
    @Test
    public void testOldestWorkCreatedTimePop() {
        taskQueue.poll();
        Assert.assertEquals(1, taskQueue.getOldestWorkCreatedTime());
        taskQueue.poll();
        Assert.assertEquals(2, taskQueue.getOldestWorkCreatedTime());
        taskQueue.poll();
        Assert.assertEquals(2, taskQueue.getOldestWorkCreatedTime());
        taskQueue.poll();
        Assert.assertEquals(Long.MAX_VALUE, taskQueue.getOldestWorkCreatedTime());
    }
}
