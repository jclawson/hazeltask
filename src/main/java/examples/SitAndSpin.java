package examples;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorService;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorServiceBuilder;


public class SitAndSpin {

    /**
     * @param args
     */
    public static void main(String[] args) {
        DistributedExecutorService svc = DistributedExecutorServiceBuilder.builder("work-test")
                .withWorkKeyAdapter(new MyWorkAdapter())
                .build();
            
            svc.startup();
    }

}
