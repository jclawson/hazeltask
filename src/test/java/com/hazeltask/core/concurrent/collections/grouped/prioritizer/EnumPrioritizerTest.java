package com.hazeltask.core.concurrent.collections.grouped.prioritizer;

import junit.framework.Assert;

import org.junit.Test;

import com.hazeltask.core.concurrent.collections.grouped.Groupable;
import com.hazeltask.core.concurrent.collections.grouped.GroupedPriorityQueueLocking;
import com.hazeltask.core.concurrent.collections.tracked.TrackCreated;
import static com.hazeltask.core.concurrent.collections.grouped.prioritizer.EnumPrioritizerTest.Priority.*;

public class EnumPrioritizerTest {
    @Test
    public void simple1() {
        EnumOrdinalPrioritizer<Priority> prioritizer = new EnumOrdinalPrioritizer<Priority>();
        GroupedPriorityQueueLocking<MyPriorityItem,Priority> queue 
            = new GroupedPriorityQueueLocking<MyPriorityItem,Priority>(prioritizer);
        
        queue.add(new MyPriorityItem(1,HIGH));
        queue.add(new MyPriorityItem(2,HIGH));
        queue.add(new MyPriorityItem(3,MEDIUM));
        queue.add(new MyPriorityItem(4,LOW));
        
        Assert.assertEquals(4, queue.size());
        
//      while(queue.size() > 0) {  System.out.println(queue.poll());  }
        
        Assert.assertEquals(1, queue.poll().id);
        Assert.assertEquals(2, queue.poll().id);
        Assert.assertEquals(3, queue.poll().id);
        Assert.assertEquals(4, queue.poll().id);
    }
    
    @Test
    public void simple2() {
        EnumOrdinalPrioritizer<Priority> prioritizer = new EnumOrdinalPrioritizer<Priority>();
        GroupedPriorityQueueLocking<MyPriorityItem,Priority> queue 
            = new GroupedPriorityQueueLocking<MyPriorityItem,Priority>(prioritizer);
        
        queue.add(new MyPriorityItem(3,MEDIUM));
        queue.add(new MyPriorityItem(1,HIGH));
        queue.add(new MyPriorityItem(4,LOW));       
        queue.add(new MyPriorityItem(2,HIGH));  
        queue.add(new MyPriorityItem(5,LOW));    
        
        Assert.assertEquals(5, queue.size());
        
//      while(queue.size() > 0) {  System.out.println(queue.poll());  }
        
        Assert.assertEquals(1, queue.poll().id);
        Assert.assertEquals(2, queue.poll().id);
        Assert.assertEquals(3, queue.poll().id);
        Assert.assertEquals(4, queue.poll().id);
        Assert.assertEquals(5, queue.poll().id);
    }
    
    public class MyPriorityItem implements Groupable<Priority>, TrackCreated {
        public final Priority group;
        public final long time;
        public final long id;
        
        public MyPriorityItem(long id, Priority group) {
            this.group = group;
            this.time = System.currentTimeMillis();
            this.id = id;
        }
        
        @Override
        public Priority getGroup() {
            return group;
        }

        @Override
        public long getTimeCreated() {
            return time;
        }

        @Override
        public String toString() {
            return "g:"+group+"\tid:"+id;
        }
        
        
    }
 
    
     static enum Priority {
        LOW,
        MEDIUM, 
        HIGH
    }
}
