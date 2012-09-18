package com.hazeltask;

import com.hazelcast.core.Hazelcast;
import com.hazeltask.batch.DeferredBundleTask;
import com.hazeltask.batch.HazelcastBatchClusterService;
import com.hazeltask.batch.IBatchClusterService;
import com.hazeltask.batch.PreventDuplicatesListener;
import com.hazeltask.batch.TaskBatchingService;
import com.hazeltask.config.BundlerConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.config.Validator;
import com.hazeltask.core.concurrent.BackoffTimer;
import com.hazeltask.core.concurrent.collections.grouped.Groupable;
import com.hazeltask.executor.DistributedExecutorService;
import com.hazeltask.executor.HazelcastExecutorTopologyService;
import com.hazeltask.executor.IExecutorTopologyService;
import com.hazeltask.executor.LocalTaskExecutorService;
import com.hazeltask.executor.StaleWorkFlushTimerTask;
import com.hazeltask.executor.WorkRebalanceTimerTask;
import com.yammer.metrics.Metrics;

public class HazeltaskBatchingExecutorBuilder<I extends Groupable> {
    private final HazeltaskConfig hazeltaskConfig;
    private final BundlerConfig<I> batchingConfig;
    
    public HazeltaskBatchingExecutorBuilder(HazeltaskConfig config, BundlerConfig<I> batchingConfig) {
        this.batchingConfig = batchingConfig;
        this.hazeltaskConfig = config;
        if(config.getHazelcast() == null) {
            config.withHazelcastInstance(Hazelcast.getDefaultInstance());
        }
        
        if(config.getMetricsRegistry() == null) {
            config.withMetricsRegistry(Metrics.defaultRegistry());
        }
        
        Validator.validate(batchingConfig);
    }
    
    public TaskBatchingService<I> build() {
        
        //TODO: we could use 1 timer thread for ALL topologies... Investigate
        BackoffTimer hazeltaskTimer = new BackoffTimer(hazeltaskConfig.getTopologyName());
        
        ITopologyService topologyService = new HazelcastTopologyService(hazeltaskConfig);
        IBatchClusterService<I> batchClusterService = new HazelcastBatchClusterService<I>(hazeltaskConfig, batchingConfig);
        HazeltaskTopology topology = new HazeltaskTopology(hazeltaskConfig, topologyService, batchClusterService);
        IExecutorTopologyService executorTopologyService = new HazelcastExecutorTopologyService(hazeltaskConfig, topology);
        
        LocalTaskExecutorService localExeutorService = new LocalTaskExecutorService(topology, batchingConfig.getExecutorConfig(), executorTopologyService);
        DistributedExecutorService eSvc = new DistributedExecutorService(topology, executorTopologyService, batchingConfig.getExecutorConfig(), null, localExeutorService);
        TaskBatchingService<I> svc = new TaskBatchingService<I>(hazeltaskConfig, batchingConfig, eSvc, topology);
        
        setup(topology, hazeltaskTimer, eSvc, topologyService, executorTopologyService, localExeutorService);
        setup(hazeltaskTimer, svc);
        
        //FIXME: we need to be careful about listener ordering
        //what if something else prevented us from adding, and we added it to the prevent dup map
        //and then the remove was never called
        //we should do the prevent duplicates listener last
        if(batchingConfig.isPreventDuplicates()) {
            PreventDuplicatesListener<I> listener = new PreventDuplicatesListener<I>(batchClusterService, batchingConfig.getBatchKeyAdapter());
            eSvc.addListener(listener);
            svc.addListener(listener);
        }
        
        Hazeltask.registerInstance(topology, svc);
        hazeltaskTimer.start();
        
        //if autoStart... we need to start
        if(batchingConfig.getExecutorConfig().isAutoStart()) {
            svc.startup();
        }
        return svc;
    }
    
    private void setup(final HazeltaskTopology topology, final BackoffTimer hazeltaskTimer, DistributedExecutorService svc, ITopologyService topologySvc, IExecutorTopologyService executorTopologyService, LocalTaskExecutorService localExeutorService) {
        final StaleWorkFlushTimerTask bundleTask = new StaleWorkFlushTimerTask(topology, svc, executorTopologyService);
        final WorkRebalanceTimerTask rebalanceTask = new WorkRebalanceTimerTask(topology, localExeutorService, executorTopologyService);
        final IsMemberReadyTimerTask getReadyMembersTask = new IsMemberReadyTimerTask(topologySvc, topology);
        
        hazeltaskConfig.getHazelcast().getCluster().addMembershipListener(getReadyMembersTask);
        
        svc.addServiceListener(new HazeltaskServiceListener<DistributedExecutorService>(){
            @Override
            public void onEndStart(DistributedExecutorService svc) {
                hazeltaskTimer.schedule(bundleTask, 1000, 30000, 2);
                hazeltaskTimer.schedule(rebalanceTask, 1000, batchingConfig.getExecutorConfig().getRebalanceTaskPeriod());
                hazeltaskTimer.schedule(getReadyMembersTask, 500, 20000);
                topology.iAmReady();
            }

            @Override
            public void onBeginShutdown(DistributedExecutorService svc) {
                hazeltaskTimer.unschedule(bundleTask);
                hazeltaskTimer.unschedule(rebalanceTask);
                hazeltaskTimer.unschedule(getReadyMembersTask);
            }      
        });
    }
    
    private void setup(final BackoffTimer hazeltaskTimer, TaskBatchingService<I> svc) {
        final DeferredBundleTask<I> bundleTask = new DeferredBundleTask<I>(batchingConfig, svc);
        svc.addServiceListener(new HazeltaskServiceListener<TaskBatchingService<I>>(){
            @Override
            public void onEndStart(TaskBatchingService<I> svc) {
                hazeltaskTimer.schedule(bundleTask, 200, 20000, 2);
            }

            @Override
            public void onBeginShutdown(TaskBatchingService<I> svc) {
                hazeltaskTimer.unschedule(bundleTask);
            }      
        });
    }
    
    
}
