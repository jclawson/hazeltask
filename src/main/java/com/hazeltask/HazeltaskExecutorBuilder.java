package com.hazeltask;

import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.concurrent.BackoffTimer;
import com.hazeltask.executor.DistributedExecutorService;
import com.hazeltask.executor.DistributedFutureTracker;
import com.hazeltask.executor.StaleWorkFlushTimerTask;
import com.hazeltask.executor.WorkRebalanceTimerTask;

public class HazeltaskExecutorBuilder {
    private final HazeltaskConfig hazeltaskConfig;
    private ExecutorConfig executorConfig = new ExecutorConfig();
    
    public HazeltaskExecutorBuilder(HazeltaskConfig config) {
        this.hazeltaskConfig = config;
    }
    
    public HazeltaskExecutorBuilder withExecutorConfig(ExecutorConfig config) {
        this.executorConfig = config;
        return this;
    }
    
    public DistributedExecutorService build() {
        //TODO: we could use 1 timer thread for ALL topologies... Investigate
        BackoffTimer hazeltaskTimer = new BackoffTimer(hazeltaskConfig.getTopologyName());
        
        HazeltaskTopology topology = new HazeltaskTopology(hazeltaskConfig, new HazelcastTopologyService(hazeltaskConfig), null);
        
        DistributedFutureTracker futureTracker = new DistributedFutureTracker();
        topology.getTopologyService().addTaskResponseMessageHandler(futureTracker);
        
        DistributedExecutorService eSvc = new DistributedExecutorService(topology, executorConfig, futureTracker);
        
        setup(topology, hazeltaskTimer, eSvc);
        
        Hazeltask.registerInstance(topology, eSvc);
        
        //if autoStart... we need to start
        if(executorConfig.isAutoStart()) {
            eSvc.startup();
        }
        return eSvc;
    }
    
    //TODO: combine with batching setup code?
    private void setup(final HazeltaskTopology topology, final BackoffTimer hazeltaskTimer, DistributedExecutorService svc) {
        final StaleWorkFlushTimerTask bundleTask = new StaleWorkFlushTimerTask(topology, svc);
        final WorkRebalanceTimerTask rebalanceTask = new WorkRebalanceTimerTask(topology);
        
        svc.addServiceListener(new HazeltaskServiceListener<DistributedExecutorService>(){
            @Override
            public void onEndStart(DistributedExecutorService svc) {
                hazeltaskTimer.schedule(bundleTask, 1000, 30000, 2);
                hazeltaskTimer.schedule(rebalanceTask, 1000, executorConfig.getRebalanceTaskPeriod());
                topology.iAmReady();
            }

            @Override
            public void onBeginShutdown(DistributedExecutorService svc) {
                topology.shutdown();
                hazeltaskTimer.unschedule(bundleTask);
                hazeltaskTimer.unschedule(rebalanceTask);
            }      
        });
    }
}
