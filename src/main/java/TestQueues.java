import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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
            .build();
        
        svc.startup();
        //ExecutorService svc = mgr.getDistributedExecutorService();
        
        Future<Integer> f = submitWork(svc, 1);
        System.out.println("getting");
        
        
        
        System.out.println("done: "+f.get());
        
        /*if(true) {
            int customerId = 0;
            for(int i = 0; i<50000; i++) {
                if(i%10 == 0)
                    customerId++;
                submitWork(svc, customerId);
            }
            System.out.println("done adding..");
            System.exit(1);
        }*/
        
    }
    
    
    public static AtomicInteger i = new AtomicInteger(0);
    public static Future<Integer> submitWork(ExecutorService svc, int customerId){
        return svc.submit(new WorkType1(i.incrementAndGet(), "customer-"+customerId));
    }
    
    public static class MyWorkAdapter implements WorkIdAdapter<WorkType1> {

        public WorkId getWorkId(WorkType1 work) {
            return new WorkId(Integer.toString(((WorkType1)work).i), ((WorkType1)work).part);
        }
        
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
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.out.println("worked "+i+" for customer "+part);
            //if(count.incrementAndGet()%100 == 0)
            //    System.out.println(count.get());
            return i;
        }
    }

}
