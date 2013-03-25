package com.hazeltask.core.concurrent.collections.grouped.prioritizer;

import junit.framework.Assert;

import org.junit.Test;

import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.concurrent.collections.grouped.GroupMetadata;
import com.hazeltask.core.concurrent.collections.grouped.GroupedPriorityQueueLocking;
import com.hazeltask.executor.metrics.ExecutorMetrics;

import data.MyGroupableItem;

public class SimplePrioritizerTest {
    @Test
    public void simple1() {
        MyPrioritizer prioritizer = new MyPrioritizer();
        GroupedPriorityQueueLocking<MyGroupableItem,Long> queue = new GroupedPriorityQueueLocking<MyGroupableItem,Long>(new ExecutorMetrics(new HazeltaskConfig()), prioritizer);
        
        queue.add(new MyGroupableItem(2,2000));
        queue.add(new MyGroupableItem(4,2));
        queue.add(new MyGroupableItem(3,3));
        queue.add(new MyGroupableItem(1,4000));
        
        Assert.assertEquals(4, queue.size());
        
//      while(queue.size() > 0) {  System.out.println(queue.poll());  }
        
        Assert.assertEquals(1, queue.poll().id);
        Assert.assertEquals(2, queue.poll().id);
        Assert.assertEquals(3, queue.poll().id);
        Assert.assertEquals(4, queue.poll().id);
    }
    
    /**
     * The group becomes the priority
     * @author jclawson
     *
     */
    private static class MyPrioritizer implements GroupPrioritizer<Long> {
        @Override
        public long computePriority(GroupMetadata<Long> metadata) {
            return metadata.getGroup();
        }
    }
}
