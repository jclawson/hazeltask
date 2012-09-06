package examples;
import java.io.Serializable;

import com.hazeltask.executor.WorkIdAdapter;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;
import com.succinctllc.hazelcast.work.WorkId;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorService;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorServiceBuilder;
import com.yammer.metrics.Metrics;


public class ExampleMain {

    /**
     * @param args
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws InterruptedException {
        DistributedExecutorService svc = DistributedExecutorServiceBuilder.builder("jasontopology")
            .withWorkKeyAdapter(new MyWorkKeyAdapter())
            .enableStatisics()
            .withThreadCount(10)
            .disableWorkers()
            .build();
        
        svc.startup();
        
        for(int i =1; i<=3000; i++) {
            svc.execute(new MyWork(i, i%3));
        }
        
        Thread.sleep(3000);
        
        for(int i =1; i<=3000; i++) {
            svc.execute(new MyWork(i, i%5));
        }
        
        int lastSize = 0;
        while(true) {
            int size = HazelcastWorkTopology.get("jasontopology").getPendingWork().size();
            if(lastSize != size) {
                lastSize = size;
                System.out.println(size);                
            }
            Thread.currentThread().sleep(100);
        }
    }
    
    public static class MyWorkKeyAdapter implements WorkIdAdapter<MyWork> {
        public WorkId createWorkId(MyWork work) {
            return new WorkId(Integer.toString(work.id), Integer.toString(work.groupId));
        }
    }
    
    public static class MyWork implements Runnable, Serializable {
        int id;
        int groupId;
        
        public MyWork(int id, int groupId) {
            this.id = id;
            this.groupId = groupId;
        }

        public void run() {
            System.out.println("Start work on "+id);
            try {
                Thread.currentThread().sleep(5000);
                throw new RuntimeException("a");
            } catch (InterruptedException e) {}
            System.out.println("Finished work on "+id);
        }
    }

}
