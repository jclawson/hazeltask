import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.succinctllc.executor.DistributedExecutorServiceManager;
import com.succinctllc.executor.ExecutorServiceManagerBuilder;
import com.succinctllc.executor.WorkKeyAdapter;
import com.succinctllc.executor.WorkReference;


public class TestQueues {

    /**
     * @param args
     */
    public static void main(String[] args) {
        DistributedExecutorServiceManager mgr = ExecutorServiceManagerBuilder.builder()
            .withWorkKeyAdapter(new MyWorkAdapter())
            .build();
        
        mgr.start();
        ExecutorService svc = mgr.getDistributedExecutorService();
        
        if(false) {
            int customerId = 0;
            for(int i = 0; i<50000; i++) {
                if(i%10 == 0)
                    customerId++;
                submitWork(svc, customerId);
            }
            System.out.println("done adding..");
            System.exit(1);
        }
        
    }
    
    
    public static AtomicInteger i = new AtomicInteger(0);
    public static void submitWork(ExecutorService svc, int customerId){
        svc.execute(new WorkType1(i.incrementAndGet(), "customer-"+customerId));
    }
    
    public static class MyWorkAdapter implements WorkKeyAdapter {

        public WorkReference getWorkKey(Runnable work) {
            return new WorkReference(Integer.toString(((WorkType1)work).i), UUID.randomUUID().toString(), ((WorkType1)work).part);
        }

        public WorkReference getWorkKey(Callable<?> work) {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
    public static class WorkType1 implements Serializable, Runnable {
        private static final long serialVersionUID = 1L;

        public static AtomicInteger count = new AtomicInteger();
        
        int i;
        String part;
        public WorkType1(int i, String part){
            this.i = i;
            this.part = part;
        }
        public void run() {
            //System.out.println("Going to to work on "+i+" for customer "+part);
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            //System.out.println("worked "+i+" for customer "+part);
            if(count.incrementAndGet()%100 == 0)
                System.out.println(count.get());
        }
    }

}
