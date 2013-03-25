package com.hazeltask.core.concurrent.collections.grouped;

import java.util.ArrayList;

import junit.framework.Assert;

import org.junit.Test;

import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.concurrent.collections.grouped.prioritizer.RoundRobinGroupPrioritizer;
import com.hazeltask.executor.metrics.ExecutorMetrics;

import data.MyGroupableItem;

public class GroupedPriorityQueueTest {
   // @Test
    public void performanceTest() {       
        GroupedPriorityQueueLockFree<MyGroupableItem,Long> queue = new GroupedPriorityQueueLockFree<MyGroupableItem,Long>(new RoundRobinGroupPrioritizer<Long>());
        
        long start,stop;
        
        System.out.println("used: "+(Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory()));
        
        start = System.currentTimeMillis();
        ArrayList<MyGroupableItem> list = new ArrayList<MyGroupableItem>();
        for(long i=0; i<900000; i++) {
            list.add(new MyGroupableItem(i%10));
        }
        stop = System.currentTimeMillis();
        
        System.out.println(stop-start);
        
        System.out.println("used: "+(Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory()));
        
        start = System.currentTimeMillis();
        for(long i=0; i<900000; i++) {
            MyGroupableItem item = new MyGroupableItem(i%10);
            queue.offer(item);
        }
        stop = System.currentTimeMillis();
        System.out.println(stop-start);
        
        System.out.println("used: "+(Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory()));
        
        
    }
    
    @Test
    public void emptyGroupHandling() {
        GroupedPriorityQueueLocking<MyGroupableItem,Long> queue = new GroupedPriorityQueueLocking<MyGroupableItem,Long>(new ExecutorMetrics(new HazeltaskConfig()), new RoundRobinGroupPrioritizer<Long>());
        //add 2 items to 10 groups
        for(long i=0; i<4; i++) {
            MyGroupableItem item = new MyGroupableItem(i);
           // System.out.println(i);
            queue.offer(item);
        }
        
        Assert.assertEquals(0L, (long)queue.poll().getGroup());
        Assert.assertEquals(1L, (long)queue.poll().getGroup());
        Assert.assertEquals(2L, (long)queue.poll().getGroup());
        
        //groups 0, 1, and 2 are empty
        
        queue.offer(new MyGroupableItem(0));
        queue.offer(new MyGroupableItem(1));
        queue.offer(new MyGroupableItem(2));
        
        //all groups present
        
        Assert.assertEquals(3L, (long)queue.poll().getGroup());
        
        //group 3 is empty
        
        Assert.assertEquals(0L, (long)queue.poll().getGroup());
        Assert.assertEquals(1L, (long)queue.poll().getGroup());
        Assert.assertEquals(2L, (long)queue.poll().getGroup());
        
        //all groups empty
        Assert.assertEquals(0, queue.size());
    }
}
