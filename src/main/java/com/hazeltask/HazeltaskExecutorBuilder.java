package com.hazeltask;

import java.util.concurrent.ExecutorService;

import com.hazelcast.core.Hazelcast;
import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.concurrent.BackoffTimer;
import com.hazeltask.executor.DistributedExecutorService;
import com.hazeltask.executor.DistributedFutureTracker;
import com.hazeltask.executor.HazelcastExecutorTopologyService;
import com.hazeltask.executor.IExecutorTopologyService;
import com.hazeltask.executor.StaleWorkFlushTimerTask;
import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.task.WorkRebalanceTimerTask;
import com.yammer.metrics.Metrics;

/**
 * FIXME: we should have a config task that pings members and checks their config
 * for compatibility with this config.  Perhaps the member ready task should do this? 
 * 
 * @author jclawson
 *
 */
public class HazeltaskExecutorBuilder {
    private final HazeltaskConfig hazeltaskConfig;
    private ExecutorConfig executorConfig = new ExecutorConfig();
    
    public HazeltaskExecutorBuilder(HazeltaskConfig config) {
        this.hazeltaskConfig = config;
        if(config.getHazelcast() == null) {
            config.withHazelcastInstance(Hazelcast.getDefaultInstance());
        }
        
        if(config.getMetricsRegistry() == null) {
            config.withMetricsRegistry(Metrics.defaultRegistry());
        }
    }
    
    public HazeltaskExecutorBuilder withExecutorConfig(ExecutorConfig config) {
        this.executorConfig = config;
        return this;
    }
    
    public ExecutorService build() {
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
        
        LocalTaskExecutorService localExeutorService = new LocalTaskExecutorService(topology, executorConfig, svc);
        DistributedExecutorService eSvc = new DistributedExecutorService(topology, svc, executorConfig, futureTracker, localExeutorService);
        
        setup(topology, hazeltaskTimer, eSvc, topologyService, svc, localExeutorService);
        
        Hazeltask.registerInstance(topology, eSvc);
        hazeltaskTimer.start();
        
        //if autoStart... we need to start
        if(executorConfig.isAutoStart()) {
            eSvc.startup();
        }
        return eSvc;
    }
    
    //TODO: combine with batching setup code?
    private void setup(final HazeltaskTopology topology, final BackoffTimer hazeltaskTimer, DistributedExecutorService svc, ITopologyService topologySvc, IExecutorTopologyService execTopSvc, LocalTaskExecutorService localExeutorService) {
        final StaleWorkFlushTimerTask bundleTask = new StaleWorkFlushTimerTask(topology, svc, execTopSvc);
        final WorkRebalanceTimerTask rebalanceTask = new WorkRebalanceTimerTask(topology, localExeutorService, execTopSvc);
        final IsMemberReadyTimerTask getReadyMembersTask = new IsMemberReadyTimerTask(topologySvc, topology);
        
        hazeltaskConfig.getHazelcast().getCluster().addMembershipListener(getReadyMembersTask);
        
        svc.addServiceListener(new HazeltaskServiceListener<DistributedExecutorService>(){
            @Override
            public void onEndStart(DistributedExecutorService svc) {
                hazeltaskTimer.schedule(bundleTask, 1000, 30000, 2);
                hazeltaskTimer.schedule(rebalanceTask, 1000, executorConfig.getRebalanceTaskPeriod());
                hazeltaskTimer.schedule(getReadyMembersTask, 500, 20000);
                topology.iAmReady();
            }

            @Override
            public void onBeginShutdown(DistributedExecutorService svc) {
                topology.shutdown();
                hazeltaskTimer.unschedule(bundleTask);
                hazeltaskTimer.unschedule(rebalanceTask);
                hazeltaskTimer.unschedule(getReadyMembersTask);
                hazeltaskTimer.stop();//this would stop after 5 min anyways
            }      
        });
    }
}
