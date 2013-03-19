package com.hazeltask.core.concurrent.collections.grouped.prioritizer;

import java.util.EnumMap;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.hazelcast.util.concurrent.ConcurrentSkipListSet;
import com.hazeltask.core.concurrent.collections.grouped.GroupMetadata;

public class LoadBalancedPriorityPrioritizerTest {
    private EnumOrdinalPrioritizer<Priority> source;
    private LoadBalancedPriorityPrioritizer<Priority> prioritizer;
    
    @Before
    public void before() {
        source = new EnumOrdinalPrioritizer<Priority>();
        prioritizer = new LoadBalancedPriorityPrioritizer<Priority>(source);
    }
    
    
    @Test
    public void test1() {
        
        ConcurrentSkipListSet<GroupMetadata<Priority>> set = new ConcurrentSkipListSet<GroupMetadata<Priority>>();
        for(Priority p : Priority.values()) {
            GroupMetadata<Priority> md = prioritize(new GroupMetadata<Priority>(p, -1));
            set.add(md);
        }
        
        EnumMap<Priority, Integer> counts = new EnumMap<Priority, Integer>(Priority.class);
        for(Priority p : Priority.values()) {
            counts.put(p, 0);
        }
        
        int RUNS = 100000;
        for(int i =0; i<RUNS; i++) {
            GroupMetadata<Priority> md = set.pollLast();
            Priority p = md.getGroup();
            //System.out.println(p.name()+"-"+md.getPriority());
            set.add(prioritize(md));
            counts.put(p, counts.get(p)+1);
        }
        
        
        for(Entry<Priority, Integer> entry : counts.entrySet()) {
            long percent = Math.round((((double)entry.getValue() / (double)RUNS)*100));
            System.out.println(entry.getKey().name()+"\t: "+percent+"%");
        }
        
        long percent1 = (long) Math.ceil(((((double)counts.get(Priority.LOW) / (double)RUNS)*100)));
        long percent2 = (long) Math.ceil(((((double)counts.get(Priority.MEDIUM) / (double)RUNS)*100)));
        long percent3 = (long) Math.ceil(((((double)counts.get(Priority.HIGH) / (double)RUNS)*100)));
        
        Assert.assertEquals(18, percent1);
        Assert.assertEquals(30, percent2);
        Assert.assertEquals(53, percent3);
        
        
        //CLEAR counts and run test again
        
        for(Priority p : Priority.values()) {
            counts.put(p, 0);
        }
        
        RUNS = 1000;
        for(int i =0; i<RUNS; i++) {
            GroupMetadata<Priority> md = set.pollLast();
            Priority p = md.getGroup();
            set.add(prioritize(md));
            counts.put(p, counts.get(p)+1);
        }
        
        percent1 = (long) Math.ceil(((((double)counts.get(Priority.LOW) / (double)RUNS)*100)));
        percent2 = (long) Math.ceil(((((double)counts.get(Priority.MEDIUM) / (double)RUNS)*100)));
        percent3 = (long) Math.ceil(((((double)counts.get(Priority.HIGH) / (double)RUNS)*100)));
        
        Assert.assertEquals(18, percent1);
        Assert.assertEquals(30, percent2);
        Assert.assertEquals(53, percent3);
    }
    
    private GroupMetadata<Priority> prioritize(GroupMetadata<Priority> g) {
        long p = prioritizer.computePriority(g);
        return new GroupMetadata<Priority>(g.getGroup(), p);
    }
    
    private static enum Priority {
        LOW,
        MEDIUM, 
        HIGH
    }
}
