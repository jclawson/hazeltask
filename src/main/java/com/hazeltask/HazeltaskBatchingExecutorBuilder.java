package com.hazeltask;

import com.hazeltask.batch.HazelcastBatchClusterService;
import com.hazeltask.batch.IBatchClusterService;
import com.hazeltask.batch.PreventDuplicatesListener;
import com.hazeltask.batch.TaskBatchingService;
import com.hazeltask.config.BundlerConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.config.Validator;
import com.hazeltask.core.concurrent.BackoffTimer;
import com.hazeltask.executor.DistributedExecutorService;
import com.hazeltask.executor.StaleWorkFlushTimerTask;
import com.succinctllc.hazelcast.work.bundler.DeferredBundleTask;

public class HazeltaskBatchingExecutorBuilder<I> {
    private final HazeltaskConfig hazeltaskConfig;
    private final BundlerConfig<I> batchingConfig;
    
    public HazeltaskBatchingExecutorBuilder(HazeltaskConfig config, BundlerConfig<I> batchingConfig) {
        this.batchingConfig = batchingConfig;
        this.hazeltaskConfig = config;
        
        Validator.validate(batchingConfig);
    }
    
    public TaskBatchingService<I> build() {
        
        //TODO: we could use 1 timer thread for ALL topologies... Investigate
        BackoffTimer hazeltaskTimer = new BackoffTimer(hazeltaskConfig.getTopologyName());
        
        IBatchClusterService<I> batchClusterService = new HazelcastBatchClusterService<I>(hazeltaskConfig);
        ITopologyService topologyService = new HazelcastTopologyService(hazeltaskConfig);
        
        HazeltaskTopology topology = new HazeltaskTopology(hazeltaskConfig, topologyService, batchClusterService);
        DistributedExecutorService eSvc = new DistributedExecutorService(topology, batchingConfig.getExecutorConfig());
        TaskBatchingService<I> svc = new TaskBatchingService<I>(hazeltaskConfig, batchingConfig, eSvc, topology);
        
        setup(hazeltaskTimer, eSvc);
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
        //if autoStart... we need to start
        if(batchingConfig.getExecutorConfig().isAutoStart()) {
            svc.startup();
        }
        return svc;
    }
    
    private void setup(final BackoffTimer hazeltaskTimer, DistributedExecutorService svc) {
        final StaleWorkFlushTimerTask bundleTask = new StaleWorkFlushTimerTask(svc);
        svc.addServiceListener(new HazeltaskServiceListener<DistributedExecutorService>(){
            @Override
            public void onEndStart(DistributedExecutorService svc) {
                hazeltaskTimer.schedule(bundleTask, 1000, 30000, 2);
            }

            @Override
            public void onBeginShutdown(DistributedExecutorService svc) {
                hazeltaskTimer.unschedule(bundleTask);
            }      
        });
    }
    
    private void setup(final BackoffTimer hazeltaskTimer, TaskBatchingService<I> svc) {
        final DeferredBundleTask<I> bundleTask = new DeferredBundleTask<I>(svc, null, null);
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
