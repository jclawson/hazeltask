package com.hazeltask.core.concurrent.collections.grouped;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Test;

import com.hazeltask.core.concurrent.collections.grouped.GroupedQueueRouter.GroupRouterAdapter;
import com.hazeltask.core.concurrent.collections.router.ListRouter;
import com.hazeltask.core.concurrent.collections.router.ListRouterFactory;
import com.hazeltask.core.concurrent.collections.router.RoundRobinRouter;
import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;
import com.hazeltask.core.concurrent.collections.tracked.TrackedPriorityBlockingQueue.TimeCreatedAdapter;

//TODO: fix this... check to make sure we visit every group
public class GroupedQueueRouterTest {
    GroupRouterAdapter<Foo> adapter;
    GroupedPriorityQueue<Foo> queue;
    
    @Before
    public void setupData() {
        GroupRouterAdapter<Foo> adapter = new GroupRouterAdapter<Foo>(new MyListRouterFactory());        
        GroupedPriorityQueue<Foo> queue = new GroupedPriorityQueue<Foo>(adapter, new TimeCreatedAdapter<Foo>(){
            public long getTimeCreated(Foo item) {
                return item.time;
            }
        });
        
        adapter.setPartitionedQueueue(queue);
        
        this.adapter = adapter;
        this.queue = queue;
    }
    
    @Test
    public void testOne() {
        expandQueue(0, 5);
        
        measure(queue.getNonEmptyGroups().size());
        
        expandQueue(5, 10);
        
        measure(queue.getNonEmptyGroups().size());
        
        expandQueue(15, 50);
        
        measure(queue.getNonEmptyGroups().size());
        
        contractQueue(15, 20);
        expandQueue(50, 96);
        contractQueue(1, 7);
        contractQueue(59, 73);
        
        measure(77);
        
        measure(queue.getNonEmptyGroups().size());
    }
    
    private Map<String, Long> measure(int iterations) {
        System.out.println("");
        Map<Integer, Integer> map = new HashMap();
        int total = iterations;
        for(int i=0; i<total; i++) {
            Queue<Foo> q = adapter.nextPartition();
            int group = q.peek().group;
            Integer count = map.get(group);
            if(count == null)
                count = 0;
            map.put(group, count+1);
        }
        
        for(Entry<Integer, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey()+": "+(((double)entry.getValue()) / total));
        }
        return null;
    }
    
    private void expandQueue(int start, int end) {
        for(int i=start; i<end; i++) {
            queue.add(new Foo(i, i));
        }
    }
    
    private void contractQueue(int start, int end) {
        for(int i=start; i<end; i++) {
            queue.getQueueByGroup(Integer.toString(i)).clear();
        }
    }
    
    
    
    
    
    
    
    
    
    
    
    static class Foo implements Groupable {
        int id;
        int group;
        long time;
        Foo(int id, int group) {
            this.id = id;
            this.group = group;
            this.time = System.currentTimeMillis();
        }
        
        public String getGroup() {
            return Integer.toString(group);
        }

        public String getUniqueIdentifier() {
            return Integer.toString(id);
        }
        
    }
    
    static class MyListRouterFactory implements ListRouterFactory<Entry<String, ITrackedQueue<Foo>>> {

        public ListRouter<Entry<String, ITrackedQueue<Foo>>> createRouter(List<Entry<String, ITrackedQueue<Foo>>> list) {
           return new RoundRobinRouter<Map.Entry<String,ITrackedQueue<Foo>>>(list);
        }

        public ListRouter<Entry<String, ITrackedQueue<Foo>>> createRouter(Callable<List<Entry<String, ITrackedQueue<Foo>>>> list) {
            return new RoundRobinRouter<Map.Entry<String,ITrackedQueue<Foo>>>(list);
        }
        
    }
}
