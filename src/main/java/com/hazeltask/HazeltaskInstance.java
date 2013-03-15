package com.hazeltask;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import com.hazeltask.batch.BatchMetrics;
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
    private final TaskBatchingService<?,Serializable,?,?>           taskBatchService;
    private final HazeltaskTopology                topology;
    private final HazeltaskConfig hazeltaskConfig;
    private final UUID hazeltaskInstanceId = UUID.randomUUID();
    private final ExecutorConfig executorConfig;
    private final BundlerConfig bundlerConfig;
    
    private final ITopologyService topologyService;
    private final IExecutorTopologyService executorTopologyService;
    private final LocalTaskExecutorService localExeutorService;
    
    /**
     * FIXME: fix  executorConfig.isDisableWorkers()
     * @param hazeltaskConfig
     */
    protected <I,ID extends Serializable,GROUP extends Serializable> HazeltaskInstance(HazeltaskConfig hazeltaskConfig) {
        this.hazeltaskConfig = hazeltaskConfig;
        
        Validator.validate(hazeltaskConfig);
        
        executorConfig = hazeltaskConfig.getExecutorConfig();
        
        //BundlerConfig<I,?,ID,GROUP> 
        bundlerConfig = hazeltaskConfig.getBundlerConfig();
        
        if(bundlerConfig != null) {
            //if are not using the bundler, then we want to make sure we are not tracking futures
            executorConfig.disableFutureSupport();
            executorConfig.withTaskIdAdapter(bundlerConfig.getBatchKeyAdapter());
        }
        

        topologyService = new HazeltaskTopologyService(hazeltaskConfig);
        IBatchClusterService<I,?,GROUP> batchClusterService = null;
        if(bundlerConfig != null) {
            batchClusterService = new HazelcastBatchClusterService<I,Serializable,GROUP>(hazeltaskConfig);
        }
        
        this.topology = new HazeltaskTopology(hazeltaskConfig, topologyService, batchClusterService);
        executorTopologyService = new HazelcastExecutorTopologyService(hazeltaskConfig, topology);
        
        
        if(!executorConfig.isDisableWorkers())
            localExeutorService = new LocalTaskExecutorService(topology, executorConfig, executorTopologyService);
        else
            localExeutorService = null;
        
        DistributedFutureTracker futureTracker = null;
        

        if(executorConfig.isFutureSupportEnabled()) {
            futureTracker = new DistributedFutureTracker();
            executorTopologyService.addTaskResponseMessageHandler(futureTracker);
        }
        
        executor = new DistributedExecutorService(topology, executorTopologyService, executorConfig, futureTracker, localExeutorService);
        
        if(bundlerConfig != null) {
            taskBatchService = new TaskBatchingService<I,Serializable,ID,GROUP>(hazeltaskConfig, executor, topology);
//          if(bundlerConfig.isPreventDuplicates()) {
//          throw new RuntimeException("Not supporting preventDuplicates this right now because it causes a lot of contention");
//        PreventDuplicatesListener<I> listener = new PreventDuplicatesListener<I>(batchClusterService, batchingConfig.getBatchKeyAdapter());
//        eSvc.addListener(listener);
//        svc.addListener(listener);
        } else {
            taskBatchService = null;
        }
    }
    
    protected void start() {
        BackoffTimer hazeltaskTimer = new BackoffTimer(hazeltaskConfig.getTopologyName(), hazeltaskConfig.getThreadFactory());
        setupDistributedExecutor(topology, hazeltaskTimer, executor, topologyService, executorTopologyService, localExeutorService);
        
        if(bundlerConfig != null) {
            setupBatching(hazeltaskTimer, taskBatchService, topology.getBatchMetrics());
        }
        
        //if autoStart... we need to start
        if(executorConfig.isAutoStart()) {
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
        final TaskRebalanceTimerTask rebalanceTask;
        if(!svc.getExecutorConfig().isDisableWorkers())
            rebalanceTask = new TaskRebalanceTimerTask(topology, localExeutorService, executorTopologyService);
        else
            rebalanceTask = null;
        final IsMemberReadyTimerTask getReadyMembersTask = new IsMemberReadyTimerTask(topologySvc, topology);
        
        //execute the getReadyMembers task immediately
        hazeltaskTimer.schedule(getReadyMembersTask, 20000, 20000);
        getReadyMembersTask.execute();
   
        hazeltaskConfig.getHazelcast().getCluster().addMembershipListener(getReadyMembersTask);
        
        svc.addServiceListener(new HazeltaskServiceListener<DistributedExecutorService<?,?>>(){
            @Override
            public void onEndStart(DistributedExecutorService svc) {
                hazeltaskTimer.schedule(bundleTask, 1000, svc.getExecutorConfig().getRecoveryProcessPollInterval(), 2);
                if(rebalanceTask != null)
                    hazeltaskTimer.schedule(rebalanceTask, 1000, hazeltaskConfig.getExecutorConfig().getLoadBalancingConfig().getRebalanceTaskPeriod());
                
                if(!svc.getExecutorConfig().isDisableWorkers())
                   topology.iAmReady();
            }

            @Override
            public void onBeginShutdown(DistributedExecutorService svc) {
                topology.shutdown();
                hazeltaskTimer.unschedule(bundleTask);
                if(rebalanceTask != null)
                    hazeltaskTimer.unschedule(rebalanceTask);
                hazeltaskTimer.unschedule(getReadyMembersTask);
                hazeltaskTimer.stop();//this would stop after 5 min anyways
            }      
        });
    }
    
    private <I,ID extends Serializable,GROUP extends Serializable> void setupBatching(final BackoffTimer hazeltaskTimer, TaskBatchingService<I,Serializable,ID,GROUP> svc, BatchMetrics metrics) {
        @SuppressWarnings("unchecked")
        final DeferredBatchTimerTask<I,GROUP> bundleTask = new DeferredBatchTimerTask<I,GROUP>((BundlerConfig<I,?,ID,GROUP>)hazeltaskConfig.getBundlerConfig(), svc, metrics);
        svc.addServiceListener(new HazeltaskServiceListener<TaskBatchingService<I,Serializable, ID,GROUP>>(){
            @Override
            public void onEndStart(TaskBatchingService<I,Serializable,ID,GROUP> svc) {
                hazeltaskTimer.schedule(bundleTask, 200, 20000, 2);
            }

            @Override
            public void onBeginShutdown(TaskBatchingService<I,Serializable,ID,GROUP> svc) {
                hazeltaskTimer.unschedule(bundleTask);
            }      
        });
    }

    public DistributedExecutorService<?,?> getExecutorService() {
        return executor;
    }

    @SuppressWarnings("unchecked")
    public <I,ID extends Serializable,GROUP extends Serializable>  TaskBatchingService<I,?,ID,GROUP> getTaskBatchService() {
        return (TaskBatchingService<I,Serializable,ID,GROUP>) taskBatchService;
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
