package examples;
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.core.Hazelcast;
import com.succinctllc.hazelcast.work.WorkId;
import com.succinctllc.hazelcast.work.WorkIdAdapter;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorService;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorServiceBuilder;


public class TestQueues {

    /**
     * @param args
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        DistributedExecutorService svc = DistributedExecutorServiceBuilder.builder("work-test")
            .withWorkKeyAdapter(new MyWorkAdapter())
            .disableWorkers()
            //.withWorkSubmissionAcknowledgement()
            .build();
        
        svc.startup();
        //ExecutorService svc = mgr.getDistributedExecutorService();
        
        //Future<Integer> f = submitWork(svc, 1);
        //System.out.println("getting");
        
        
        
        //System.out.println("done: "+f.get());
        int customerId = 0;
        long current = System.currentTimeMillis();
        for(int i = 0; i<5; i++) {
            if(i%1000 == 0)
                customerId++;
            submitWork(svc, customerId);
        }
        
        System.out.println("done adding... took "+(System.currentTimeMillis() - current)+"ms");
        
        Thread.currentThread().sleep(1000);
        
        long time = System.currentTimeMillis();
        Future<Integer> f = submitWork(svc, 7);
        System.out.println("Done getting "+f.get()+" in "+(System.currentTimeMillis() - time)+"ms");
        
        time = System.currentTimeMillis();
        f = submitWork(svc, 7);
        System.out.println("Done getting "+f.get()+" in "+(System.currentTimeMillis() - time)+"ms");
        
        time = System.currentTimeMillis();
        f = submitWork(svc, 7);
        System.out.println("Done getting "+f.get()+" in "+(System.currentTimeMillis() - time)+"ms");
        
        
        System.out.println("------------------");
        ExecutorService svc2 = Hazelcast.getExecutorService();
        
        time = System.currentTimeMillis();
        f = submitWork(svc2, 7);
        System.out.println("Done getting "+f.get()+" in "+(System.currentTimeMillis() - time)+"ms");
        
        time = System.currentTimeMillis();
        f = submitWork(svc2, 7);
        System.out.println("Done getting "+f.get()+" in "+(System.currentTimeMillis() - time)+"ms");
        
        time = System.currentTimeMillis();
        f = submitWork(svc2, 7);
        System.out.println("Done getting "+f.get()+" in "+(System.currentTimeMillis() - time)+"ms");
        
        
        
        //System.exit(1);
        
    }
    
    
    public static AtomicInteger i = new AtomicInteger(0);
    public static Future<Integer> submitWork(ExecutorService svc, int customerId){
        return svc.submit(new WorkType1(i.incrementAndGet(), "customer-"+customerId));
    }
    
    
    
    public static class WorkType1 implements Serializable, Callable<Integer> {
        private static final long serialVersionUID = 1L;

        public static AtomicInteger count = new AtomicInteger();
        
        int i;
        String part;
        public WorkType1(int i, String part){
            this.i = i;
            this.part = part;
        }
        public Integer call() throws Exception {
            System.out.println("Going to to work on "+i+" for customer "+part);
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
            System.out.println("worked "+i+" for customer "+part);
            //if(count.incrementAndGet()%100 == 0)
            //    System.out.println(count.get());
            return i;
        }
    }

}
