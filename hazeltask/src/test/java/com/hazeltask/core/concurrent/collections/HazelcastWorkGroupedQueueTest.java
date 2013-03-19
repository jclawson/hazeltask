package com.hazeltask.core.concurrent.collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.hazeltask.core.concurrent.collections.grouped.GroupedPriorityQueueLocking;
import com.hazeltask.core.concurrent.collections.grouped.prioritizer.RoundRobinGroupPrioritizer;
import com.hazeltask.executor.task.HazeltaskTask;

public class HazelcastWorkGroupedQueueTest {
    
    private GroupedPriorityQueueLocking<HazeltaskTask<String,String>, String> taskQueue;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setupData() {
        taskQueue = new GroupedPriorityQueueLocking<HazeltaskTask<String,String>, String>(new RoundRobinGroupPrioritizer<String>());
        
        HazeltaskTask<String,String> work1 = mock(HazeltaskTask.class);
        HazeltaskTask<String,String> work2 = mock(HazeltaskTask.class);
        HazeltaskTask<String,String> work3 = mock(HazeltaskTask.class);
        HazeltaskTask<String,String> work4 = mock(HazeltaskTask.class);
        
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
        Assert.assertEquals(1L, (long)taskQueue.getOldestQueueTime());
    }
    
    @Test
    public void testOldestWorkCreatedTimePop() {
        Assert.assertEquals(1L, (long)taskQueue.getOldestQueueTime());
        taskQueue.poll();
        Assert.assertEquals(2L, (long)taskQueue.getOldestQueueTime());
        taskQueue.poll();
        Assert.assertEquals(3L, (long)taskQueue.getOldestQueueTime());
        taskQueue.poll();
        Assert.assertEquals(4L, (long)taskQueue.getOldestQueueTime());
        taskQueue.poll();
        Assert.assertNull(taskQueue.getOldestQueueTime());
    }
}
