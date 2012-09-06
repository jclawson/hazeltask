package examples;

import com.succinctllc.hazelcast.work.executor.DistributedExecutorService;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorServiceBuilder;

import examples.ExampleMain.MyWorkKeyAdapter;


public class SitAndSpinMain {

    public static void main(String[] args) {
        DistributedExecutorService svc = DistributedExecutorServiceBuilder.builder("jasontopology")
                .withWorkKeyAdapter(new MyWorkKeyAdapter())
                .enableStatisics()
                .withThreadCount(10)
                .build();
        
        svc.startup();
    }

}
