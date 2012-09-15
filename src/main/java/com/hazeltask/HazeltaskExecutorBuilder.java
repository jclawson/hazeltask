package com.hazeltask;

import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.concurrent.BackoffTimer;
import com.hazeltask.executor.DistributedExecutorService;
import com.hazeltask.executor.DistributedFutureTracker;
import com.hazeltask.executor.HazelcastExecutorTopologyService;
import com.hazeltask.executor.IExecutorTopologyService;
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
        
        //FIXME: how should I track readyMembers?
        // - the topologyService needs to know readyMembers
        // - I wanted the topology to track them because they are part of the "Topology"
        // - The topologyService also has the method to query for readyMembers
        // - But I want to avoid tight coupling
        
        //     I think the answer here is to remove the readyMember query from the topologyService
        //     and rename the topology service to something else... err... keep only readyMember query
        //     in the topology service.
        
        //        lets rename topology service too.... 
        //        IExecutorTopologyService, IBatchTopologyService, ITopologyService
        
        ITopologyService topologyService = new HazelcastTopologyService(hazeltaskConfig);
        HazeltaskTopology topology = new HazeltaskTopology(hazeltaskConfig, topologyService, null);
        IExecutorTopologyService svc = new HazelcastExecutorTopologyService(hazeltaskConfig, topology);
        
        DistributedFutureTracker futureTracker = new DistributedFutureTracker();
        svc.addTaskResponseMessageHandler(futureTracker);
        
        DistributedExecutorService eSvc = new DistributedExecutorService(topology, svc, executorConfig, futureTracker);
        
        setup(topology, hazeltaskTimer, eSvc, svc);
        
        Hazeltask.registerInstance(topology, eSvc);
        
        //if autoStart... we need to start
        if(executorConfig.isAutoStart()) {
            eSvc.startup();
        }
        return eSvc;
    }
    
    //TODO: combine with batching setup code?
    private void setup(final HazeltaskTopology topology, final BackoffTimer hazeltaskTimer, DistributedExecutorService svc, IExecutorTopologyService execTopSvc) {
        final StaleWorkFlushTimerTask bundleTask = new StaleWorkFlushTimerTask(topology, svc, execTopSvc);
        final WorkRebalanceTimerTask rebalanceTask = new WorkRebalanceTimerTask(topology, execTopSvc);
        
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
