package com.hazeltask.core.concurrent.collections.tracked;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import data.SimpleItem;

public class TrackedPriorityBlockingQueueTest {
    TrackedPriorityBlockingQueue<SimpleItem> queue;
    
    SimpleItem item100 = new SimpleItem(1, 100);
    SimpleItem item101 = new SimpleItem(1, 101);
    SimpleItem item102 = new SimpleItem(1, 102);
    SimpleItem item103 = new SimpleItem(1, 103);
    SimpleItem item1   = new SimpleItem(1, 1);
    SimpleItem item2   = new SimpleItem(1, 2);
    SimpleItem item3   = new SimpleItem(1, 3);
    
    @Before
    public void before() {
        queue = new TrackedPriorityBlockingQueue<SimpleItem>();
        queue.offer(item100);
        
        queue.offer(item102);
        queue.offer(item103);
        
        queue.offer(item1);
        queue.offer(item3);
        queue.offer(item2);
        
        queue.offer(item101);      
    }
    
    @Test
    public void expectedPollOrder() {
        assertEquals("expected poll order", item1, queue.poll());
        assertEquals("expected poll order", item2, queue.poll());
        assertEquals("expected poll order", item3, queue.poll());
        assertEquals("expected poll order", item100, queue.poll());
        assertEquals("expected poll order", item101, queue.poll());
        assertEquals("expected poll order", item102, queue.poll());
        assertEquals("expected poll order", item103, queue.poll());
    }
    
    @Test
    public void getOldestTime() {
        assertEquals(1L, (long)queue.getOldestItemTime());
    }
    
    @Test
    public void getLastAddedTime() throws InterruptedException {
        Thread.sleep(10);
        //unless a machine is really really slow... this should be ok
        long now = System.currentTimeMillis();
        queue.offer(item101);
        
        assertTrue(timeBuffer(now, (long)queue.getLastAddedTime()));
    }
    
    @Test
    public void getLastRemovedTime() throws InterruptedException {
        Thread.sleep(10);
        //unless a machine is really really slow... this should be ok
        long now = System.currentTimeMillis();
        queue.poll();
        
        assertTrue(timeBuffer(now, (long)queue.getLastRemovedTime()));
    }
    
    @Test
    public void getOldestTimeNull() {
        queue.clear();
        Assert.assertNull("oldest time should be null", queue.getOldestItemTime());
    }
    
    @Test
    public void getLastAddedTimeNull() {
        TrackedPriorityBlockingQueue<SimpleItem> queue = new TrackedPriorityBlockingQueue<SimpleItem>();
        queue.clear();
        Assert.assertNull("oldest time should be null", queue.getLastAddedTime());
    }
    
    @Test
    public void getLastRemovedTimeNull() {
        queue.clear();
        Assert.assertNull("oldest time should be null", queue.getLastRemovedTime());
    }
    
    @Test
    public void remove() throws InterruptedException {
        Thread.sleep(10);
        long now = System.currentTimeMillis();
        boolean removed = queue.remove(item100);
        Assert.assertTrue("item wasn't removed",removed);
        assertTrue("time removed not within buffer", timeBuffer(now, (long)queue.getLastRemovedTime())); 
    }
    
    /**
     * time can vary a little so allow a buffer in testing 
     * 
     * @param expected
     * @param actual
     * @param buffer
     * @return
     */
    private boolean timeBuffer(long expected, long actual) {
        long buffer = 2;
        return actual <= (expected+buffer) && actual >= (expected-buffer);
    }
}
