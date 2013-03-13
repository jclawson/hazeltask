package com.hazeltask.core.concurrent.collections.grouped.prioritizer;

import junit.framework.Assert;

import org.junit.Test;

import com.hazelcast.util.concurrent.ConcurrentSkipListSet;
import com.hazeltask.core.concurrent.collections.grouped.GroupedPriorityQueueLocking;

import data.MyGroupableItem;

public class RoundRobinGroupPrioritizerTest {
    @Test
    public void testSkipList() {
        ConcurrentSkipListSet<Elem> set = new ConcurrentSkipListSet<Elem>();
        set.add(new Elem(1,0));
        set.add(new Elem(2,0));
        set.add(new Elem(3,0));
        set.add(new Elem(4,0));
        
        Assert.assertEquals(4, set.size());
        
        Assert.assertEquals(1, set.pollLast().id);
        Assert.assertEquals(2, set.pollLast().id);
        Assert.assertEquals(3, set.pollLast().id);
        Assert.assertEquals(4, set.pollLast().id);
       
    }
    
    @Test
    public void testSkipList2() {
        ConcurrentSkipListSet<Elem> set = new ConcurrentSkipListSet<Elem>();
        set.add(new Elem(1,0));
        set.add(new Elem(2,0));
        set.add(new Elem(3,0));
        set.add(new Elem(4,0));
        
        Assert.assertEquals(4, set.size());
        
        Elem elem = set.pollLast();
        set.add(elem);
        
        Assert.assertEquals(2, set.pollLast().id);
        Assert.assertEquals(3, set.pollLast().id);
        Assert.assertEquals(4, set.pollLast().id);
        Assert.assertEquals(1, set.pollLast().id);
       
    }
    
    @Test
    public void testRoundRobin() {
        ConcurrentSkipListSet<Elem> set = new ConcurrentSkipListSet<Elem>();
        RoundRobinGroupPrioritizer<Elem> prioritizer = new RoundRobinGroupPrioritizer<Elem>();
        
        set.add(new Elem(1,prioritizer.computePriority(null)));
        set.add(new Elem(2,prioritizer.computePriority(null)));
        set.add(new Elem(3,prioritizer.computePriority(null)));
        set.add(new Elem(4,prioritizer.computePriority(null)));
        
        Assert.assertEquals(4, set.size());
        
        Assert.assertEquals(1, set.pollLast().id);
        Assert.assertEquals(2, set.pollLast().id);
        Assert.assertEquals(3, set.pollLast().id);
        Assert.assertEquals(4, set.pollLast().id);
       
    }
    
    /**
     * This test is meant to assure that the round robin prioritizer results in insertion order
     * prioritization.
     */
    @Test
    public void testWithGroupedPriorityQueue() {
        RoundRobinGroupPrioritizer<Long> prioritizer = new RoundRobinGroupPrioritizer<Long>();
        GroupedPriorityQueueLocking<MyGroupableItem,Long> queue = new GroupedPriorityQueueLocking<MyGroupableItem,Long>(prioritizer);
        
        queue.add(new MyGroupableItem(1,1));
        queue.add(new MyGroupableItem(2,2));
        queue.add(new MyGroupableItem(3,3));
        queue.add(new MyGroupableItem(4,4));
        
        Assert.assertEquals(4, queue.size());
//        
//        while(queue.size() > 0) {
//            MyGroupableItem item = queue.poll();
//            System.out.println(queue.poll());
//        }
        
        Assert.assertEquals(1, queue.poll().id);
        Assert.assertEquals(2, queue.poll().id);
        Assert.assertEquals(3, queue.poll().id);
        Assert.assertEquals(4, queue.poll().id);
    }
    
    @Test
    public void testWithGroupedPriorityQueue2() {
        RoundRobinGroupPrioritizer<Long> prioritizer = new RoundRobinGroupPrioritizer<Long>();
        GroupedPriorityQueueLocking<MyGroupableItem,Long> queue = new GroupedPriorityQueueLocking<MyGroupableItem,Long>(prioritizer);
        
        queue.add(new MyGroupableItem(1,1));
        queue.add(new MyGroupableItem(2,2));
        queue.add(new MyGroupableItem(3,3));
        queue.add(new MyGroupableItem(4,4));
        
        Assert.assertEquals(4, queue.size());
        
//        while(queue.size() > 0) {  System.out.println(queue.poll());  }
        
        Assert.assertEquals(1, queue.poll().id);
        Assert.assertEquals(2, queue.poll().id);
        Assert.assertEquals(3, queue.poll().id);
        
        queue.add(new MyGroupableItem(20,20));
        
        Assert.assertEquals(4, queue.poll().id);
        Assert.assertEquals(20, queue.poll().id);
    }

    private static class Elem implements Comparable<Elem> {
        int  id;
        long priority;

        Elem(int id, long priority) {
            this.id = id;
            this.priority = priority;
        }

        /**
         * It seems as though the ConcurrentSkipListSet requires the compareTo to be 
         * consistent with equals.  ie: if you return 0, it thinks its equal!
         */
        @Override
        public int compareTo(Elem o) {
            int comparison = ((Long)priority).compareTo(o.priority);
            if(comparison == 0) {
                if(((Integer)id).compareTo(o.id) != 0) {
                    return -1;
                }
            }
            return comparison;
        }

        @Override
        public int hashCode() {
            return id;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            Elem other = (Elem) obj;
            if (id != other.id) return false;
            return true;
        }
        
        

    }
}
