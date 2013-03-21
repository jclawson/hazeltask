package com.hazeltask.config;

import java.io.Serializable;

import com.hazeltask.executor.task.DefaultTaskIdAdapter;
import com.hazeltask.executor.task.TaskIdAdapter;

public class ExecutorConfig<GROUP extends Serializable> {
    protected boolean          disableWorkers           = false;
    protected int              corePoolSize             = 4;
    protected int              maxPoolSize              = 4;
    protected long             maxThreadKeepAlive       = 60000;

    @SuppressWarnings("unchecked")
    protected TaskIdAdapter<?, GROUP>    taskIdAdapter  = (TaskIdAdapter<?, GROUP>) new DefaultTaskIdAdapter();
    protected boolean          autoStart                = true;
    private boolean            enableFutureTracking     = true;
    
    private long               recoveryProcessPollInterval = 30000;
    
    private ExecutorLoadBalancingConfig<GROUP> executorLoadBalancingConfig = new ExecutorLoadBalancingConfig<GROUP>();
    
    /**
     * Please consider using one of the ExecutorConfigs static factory methods
     */
    public ExecutorConfig() {
        
    }

//    Future use
//    public ExecutorConfig<GROUP> useAsyncronousTaskDistribution() {
//
//        return this;
//    }

    /**
     * This option is false by default.
     * <p>
     * If you don't want a particular member to work on tasks, enable this option
     * 
     * @param disableWorkers
     * @return
     */
    public ExecutorConfig<GROUP> withDisableWorkers(boolean disableWorkers) {
        this.disableWorkers = disableWorkers;
        return this;
    }

    /**
     * This option is not enabled by default
     * <p>
     * If you don't want a particular member to work on tasks, enable this option
     * 
     * @param disableWorkers
     * @return
     */
    public ExecutorConfig<GROUP> disableWorkers() {
        this.disableWorkers = true;
        return this;
    }
    
    /**
     * This option is not enabled by default
     * <p>
     * If you don't wish to track distributed futures you can save a little bit of work
     * by disabling them here.  This will cause this member to not push work responses 
     * into the Hazelcast response topic.
     * <p>
     * WARNING: never turn off future support on a member if other members in the cluster
     * rely on futures.  They will wait forever...
     * 
     * @return
     */
    public ExecutorConfig<GROUP> disableFutureSupport() {
        this.enableFutureTracking = false;
        return this;
    }

    public boolean isFutureSupportEnabled() {
        return this.enableFutureTracking;
    }

    /**
     * The number of threads to use to work on tasks for this member
     * 
     * @param threadCount
     * @return
     */
    public ExecutorConfig<GROUP> withThreadCount(int threadCount) {
        this.corePoolSize = threadCount;
        return this;
    }

    /**
     * This adapter is used to take a Task and identify the GROUP it belongs to
     * 
     * @param taskIdAdapter
     * @return
     */
    public ExecutorConfig<GROUP> withTaskIdAdapter(TaskIdAdapter<?,GROUP> taskIdAdapter) {
        this.taskIdAdapter = taskIdAdapter;
        return this;
    }
    
    /**
     * This interval is the period in which the write ahead log is checked to see if any 
     * lost tasks are present.
     * 
     * @param intervalMillis
     * @return
     */
    public ExecutorConfig<GROUP> withRecoveryProcessPollInterval(long intervalMillis) {
        this.recoveryProcessPollInterval = intervalMillis;
        return this;
    }
    
    public long getRecoveryProcessPollInterval() {
        return this.recoveryProcessPollInterval;
    }

    /**
     * By default we will automatically startup the task system when its
     * created. Some developers may want to delay the startup, and handle in
     * manually. For example, waiting until the web server has fully started.
     * 
     * @param autoStart
     * @return
     */
    public ExecutorConfig<GROUP> withAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
        return this;
    }

    /**
     * @see withAutoStart
     */
    public ExecutorConfig<GROUP> disableAutoStart() {
        this.autoStart = false;
        return this;
    }

    public boolean isDisableWorkers() {
        return disableWorkers;
    }
   
    public int getThreadCount() {
        return corePoolSize;
    }
  
    @SuppressWarnings("unchecked")
    public TaskIdAdapter<? super Object, GROUP> getTaskIdAdapter() {
        return (TaskIdAdapter<? super Object, GROUP>) taskIdAdapter;
    }
 
    public boolean isAutoStart() {
        return autoStart;
    }
    
    public int getMaxThreadPoolSize() {
        return this.maxPoolSize;
    }
    
    public long getMaxThreadKeepAlive() {
        return maxThreadKeepAlive;
    }
    
    public ExecutorConfig<GROUP> withLoadBalancingConfig(ExecutorLoadBalancingConfig<GROUP> config) {
        this.executorLoadBalancingConfig = config;
        return this;
    }
    
    public ExecutorLoadBalancingConfig<GROUP> getLoadBalancingConfig() {
        return this.executorLoadBalancingConfig;
    }
}
