package com.hazeltask.core.concurrent.collections.grouped;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.google.common.collect.Lists;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.concurrent.collections.grouped.prioritizer.RoundRobinGroupPrioritizer;
import com.hazeltask.executor.metrics.ExecutorMetrics;

import data.MyGroupableItem;

public class GroupedPriorityQueuePerformanceTest {
    //@Rule
    public BenchmarkRule benchmarkRun = new BenchmarkRule();
    
    static List<MyGroupableItem> items1 = Lists.newArrayList();
    static List<MyGroupableItem> items2 = Lists.newArrayList();
    static List<MyGroupableItem> items3 = Lists.newArrayList();
    static List<MyGroupableItem> bigList = Lists.newArrayList();
    
    @BeforeClass
    public static void prepare() {
        for(long i=0; i<50000; i++) {
            bigList.add(new MyGroupableItem(i%10));
        }
        
        for(long i=0; i<10000; i++) {
            items1.add(new MyGroupableItem(i%10));
            items2.add(new MyGroupableItem(i%20));
            items3.add(new MyGroupableItem(i%30));
        }
        
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
    }
    
    
    @Test
    @BenchmarkOptions(benchmarkRounds = 50, warmupRounds = 5)
    public void offer_lock() {
        GroupedPriorityQueueLocking<MyGroupableItem,Long> queue = new GroupedPriorityQueueLocking<MyGroupableItem,Long>(new ExecutorMetrics(new HazeltaskConfig()), new RoundRobinGroupPrioritizer<Long>());
        runOfferTest(queue);
    }
    
    private void runOfferTest(BlockingQueue<MyGroupableItem> queue) {
        for(MyGroupableItem item : bigList) {
            queue.offer(item);
        }
    }
    
    private void runPollTest(BlockingQueue<MyGroupableItem> queue) {
        PollThread t1 = new PollThread(queue);
        PollThread t2 = new PollThread(queue);
        PollThread t3 = new PollThread(queue);
        PollThread t4 = new PollThread(queue);
        
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        
        for(MyGroupableItem item : bigList) {
            queue.offer(item);
        }
        
        System.out.println("Size: "+queue.size());
        
        int i = 0;
        while(queue.size() != 0) {
            try {
                System.out.println(i+":\t "+queue.size());
                Thread.sleep(500);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            if(i++ > 20) {
                System.out.println("...");
                break;
            }
        }
        
        System.out.println("----------");
        
           t1.stop = true;
           t2.stop = true;
           t3.stop = true;
           t4.stop = true;
        
        t1.interrupt();
        t2.interrupt();
        t3.interrupt();
        t4.interrupt();
        
//        t1.stop();
//        t2.stop();
//        t3.interrupt();
//        t4.interrupt();
    }
    
    @Test
    @BenchmarkOptions(benchmarkRounds = 50, warmupRounds = 5, concurrency = -1)
    public void poll_locking() {
        GroupedPriorityQueueLocking<MyGroupableItem,Long> queue = new GroupedPriorityQueueLocking<MyGroupableItem,Long>(new ExecutorMetrics(new HazeltaskConfig()), new RoundRobinGroupPrioritizer<Long>());
        
        runPollTest(queue);

        Assert.assertEquals(0, queue.size());
        
//        System.out.println(queue.size());
    }
    
 //   @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 0)
    public void offerAndPoll_locking() {
        GroupedPriorityQueueLocking<MyGroupableItem,Long> queue = new GroupedPriorityQueueLocking<MyGroupableItem,Long>(new ExecutorMetrics(new HazeltaskConfig()), new RoundRobinGroupPrioritizer<Long>());
        runOfferAndPollTest(queue);
        Assert.assertEquals(0, queue.size());
    }
    
    private void runOfferAndPollTest(BlockingQueue<MyGroupableItem> queue) {
        new PollThread(queue).start();
        new PollThread(queue).start();
        new PollThread(queue).start();
        
        new OfferThread(queue, items1).start();
        new OfferThread(queue, items2).start();
        new OfferThread(queue, items3).start();
        
        for(MyGroupableItem item : bigList) {
            queue.offer(item);
        }
        
        try {
            Thread.currentThread().sleep(30);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    class OfferThread extends Thread {
        BlockingQueue q;
        List<MyGroupableItem> items;
        OfferThread(BlockingQueue q, List<MyGroupableItem> items) {
            this.q = q;
            this.items = items;
        }
        
        @Override
        public void run() {
             for(MyGroupableItem item : items) {
                 q.offer(item);
             }
        }
    }
    
    class PollThread extends Thread {
        volatile boolean stop = false;
        BlockingQueue q;
        PollThread(BlockingQueue q) {
            this.q = q;
            this.setDaemon(true);
        }
        
        @Override
        public void run() {
            while(true) {
                if(Thread.interrupted() || stop) {
//                    System.out.println("exit1");
                    return;
                }
                try {
                        q.take();
                } catch (InterruptedException e) {
//                    System.out.println("exit");
                    return;
                }
            }
        }
    }
}
