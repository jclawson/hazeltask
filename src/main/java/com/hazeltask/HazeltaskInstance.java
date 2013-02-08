package com.hazeltask;

import java.util.UUID;
import java.util.concurrent.ExecutorService;

import com.hazeltask.batch.DeferredBatchTimerTask;
import com.hazeltask.batch.HazelcastBatchClusterService;
import com.hazeltask.batch.IBatchClusterService;
import com.hazeltask.batch.TaskBatchingService;
import com.hazeltask.config.BundlerConfig;
import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.config.Validator;
import com.hazeltask.core.concurrent.BackoffTimer;
import com.hazeltask.executor.DistributedExecutorService;
import com.hazeltask.executor.DistributedFutureTracker;
import com.hazeltask.executor.HazelcastExecutorTopologyService;
import com.hazeltask.executor.IExecutorTopologyService;
import com.hazeltask.executor.StaleTaskFlushTimerTask;
import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.task.TaskRebalanceTimerTask;

public class HazeltaskInstance {
    private final DistributedExecutorService       executor;
    private final TaskBatchingService<?>           taskBatchService;
    private final HazeltaskTopology                topology;
    private final HazeltaskConfig hazeltaskConfig;
    private final UUID hazeltaskInstanceId = UUID.randomUUID();
    
    
    protected <I> HazeltaskInstance(HazeltaskConfig hazeltaskConfig) {
        this.hazeltaskConfig = hazeltaskConfig;
        
        Validator.validate(hazeltaskConfig);
        
        ExecutorConfig executorConfig = hazeltaskConfig.getExecutorConfig();
        BundlerConfig<I> bundlerConfig = hazeltaskConfig.getBundlerConfig();
        
        BackoffTimer hazeltaskTimer = new BackoffTimer(hazeltaskConfig.getTopologyName());
        ITopologyService topologyService = new HazeltaskTopologyService(hazeltaskConfig);
        IBatchClusterService<I> batchClusterService = null;
        if(bundlerConfig != null) {
            batchClusterService = new HazelcastBatchClusterService<I>(hazeltaskConfig);
        }
        
        this.topology = new HazeltaskTopology(hazeltaskConfig, topologyService, batchClusterService);
        IExecutorTopologyService executorTopologyService = new HazelcastExecutorTopologyService(hazeltaskConfig, topology);
        LocalTaskExecutorService localExeutorService = new LocalTaskExecutorService(topology, executorConfig, executorTopologyService);
        
        DistributedFutureTracker futureTracker = null;
        
        //if we are not using the bundler, then we want to make sure we are tracking futures
        if(bundlerConfig == null) {
            futureTracker = new DistributedFutureTracker();
            executorTopologyService.addTaskResponseMessageHandler(futureTracker);
        }
        
        executor = new DistributedExecutorService(topology, executorTopologyService, executorConfig, futureTracker, localExeutorService);
        
        setupDistributedExecutor(topology, hazeltaskTimer, executor, topologyService, executorTopologyService, localExeutorService);
        
        if(bundlerConfig != null) {
            taskBatchService = new TaskBatchingService<I>(hazeltaskConfig, executor, topology);
            setupBatching(hazeltaskTimer, taskBatchService);
            
//            if(bundlerConfig.isPreventDuplicates()) {
//                throw new RuntimeException("Not supporting preventDuplicates this right now because it causes a lot of contention");
//              PreventDuplicatesListener<I> listener = new PreventDuplicatesListener<I>(batchClusterService, batchingConfig.getBatchKeyAdapter());
//              eSvc.addListener(listener);
//              svc.addListener(listener);
//            }
        } else {
            taskBatchService = null;
        }
        
        
        
        
        //if autoStart... we need to start
        if(executorConfig.isAutoStart() && !executorConfig.isDisableWorkers()) {
//            TODO: is it useful to only auto-start after hazelcast is done starting?
//            LifecycleService lifecycleService = hazeltaskConfig.getHazelcast().getLifecycleService();
//            final ReentrantLock autoStartLock = new ReentrantLock();
//            LifecycleListener autoStartListener = new LifecycleListener() {                
//                public void stateChanged(LifecycleEvent event) {
//                    if(!lifecycleService.isRunning()) {
//                        autoStartLock.lock();
//                    }
//                }
//            };
            
//            hazeltaskConfig.getHazelcast().getLifecycleService().addLifecycleListener(autoStartListener);
            
            if(taskBatchService != null) {
                taskBatchService.startup();
            } else {
                executor.startup();
            }
        }
    }
    
    private void setupDistributedExecutor(final HazeltaskTopology topology, final BackoffTimer hazeltaskTimer, DistributedExecutorService svc, ITopologyService topologySvc, IExecutorTopologyService executorTopologyService, LocalTaskExecutorService localExeutorService) {
        final StaleTaskFlushTimerTask bundleTask = new StaleTaskFlushTimerTask(topology, svc, executorTopologyService);
        final TaskRebalanceTimerTask rebalanceTask = new TaskRebalanceTimerTask(topology, localExeutorService, executorTopologyService);
        final IsMemberReadyTimerTask getReadyMembersTask = new IsMemberReadyTimerTask(topologySvc, topology);
        
        hazeltaskConfig.getHazelcast().getCluster().addMembershipListener(getReadyMembersTask);
        
        svc.addServiceListener(new HazeltaskServiceListener<DistributedExecutorService>(){
            @Override
            public void onEndStart(DistributedExecutorService svc) {
                hazeltaskTimer.schedule(bundleTask, 1000, 30000, 2);
                hazeltaskTimer.schedule(rebalanceTask, 1000, hazeltaskConfig.getExecutorConfig().getRebalanceTaskPeriod());
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
    
    private <I> void setupBatching(final BackoffTimer hazeltaskTimer, TaskBatchingService<I> svc) {
        @SuppressWarnings("unchecked")
        final DeferredBatchTimerTask<I> bundleTask = new DeferredBatchTimerTask<I>((BundlerConfig<I>)hazeltaskConfig.getBundlerConfig(), svc);
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

    public ExecutorService getExecutorService() {
        return executor;
    }

    @SuppressWarnings("unchecked")
    public <I> TaskBatchingService<I> getTaskBatchService() {
        return (TaskBatchingService<I>) taskBatchService;
    }

    public HazeltaskTopology getTopology() {
        return topology;
    }

    public HazeltaskConfig getHazeltaskConfig() {
        return hazeltaskConfig;
    }
    
    public UUID getId() {
        return hazeltaskInstanceId;
    }
    
    
}
