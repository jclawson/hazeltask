package com.hazeltask.core.concurrent.collections.grouped;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.junit.Test;

import com.hazeltask.core.concurrent.collections.grouped.GroupedQueueRouter.GroupRouterAdapter;
import com.hazeltask.core.concurrent.collections.router.ListRouter;
import com.hazeltask.core.concurrent.collections.router.ListRouterFactory;
import com.hazeltask.core.concurrent.collections.router.RoundRobinRouter;
import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;
import com.hazeltask.core.concurrent.collections.tracked.TrackedPriorityBlockingQueue.TimeCreatedAdapter;

import data.MyGroupableItem;

public class GroupedPriorityQueueTest {
    @Test
    public void performanceTest() {
        GroupRouterAdapter<MyGroupableItem,Long> adapter = new GroupRouterAdapter<MyGroupableItem,Long>(new MyListRouterFactory());        
        GroupedPriorityQueue<MyGroupableItem,Long> queue = new GroupedPriorityQueue<MyGroupableItem,Long>(adapter, new TimeCreatedAdapter<MyGroupableItem>(){
            public long getTimeCreated(MyGroupableItem item) {
                return 0;
            }
        });
        adapter.setPartitionedQueueue(queue);
        
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
    
    
    
    static class MyListRouterFactory implements ListRouterFactory<Entry<Long, ITrackedQueue<MyGroupableItem>>> {

        public ListRouter<Entry<Long, ITrackedQueue<MyGroupableItem>>> createRouter(List<Entry<Long, ITrackedQueue<MyGroupableItem>>> list) {
           return new RoundRobinRouter<Map.Entry<Long,ITrackedQueue<MyGroupableItem>>>(list);
        }

        public ListRouter<Entry<Long, ITrackedQueue<MyGroupableItem>>> createRouter(Callable<List<Entry<Long, ITrackedQueue<MyGroupableItem>>>> list) {
            return new RoundRobinRouter<Map.Entry<Long,ITrackedQueue<MyGroupableItem>>>(list);
        }
        
    }
}
