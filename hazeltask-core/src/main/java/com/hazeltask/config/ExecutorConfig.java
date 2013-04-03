package com.hazeltask.config;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import com.hazeltask.executor.task.DefaultTaskIdAdapter;
import com.hazeltask.executor.task.TaskIdAdapter;

public class ExecutorConfig<GROUP extends Serializable> {
    protected boolean          disableWorkers              = false;
    protected int              corePoolSize                = 4;
    protected int              maxPoolSize                 = 4;
    protected long             maxThreadKeepAlive          = 60000;

    @SuppressWarnings("unchecked")
    protected TaskIdAdapter<?, GROUP>    taskIdAdapter     = (TaskIdAdapter<?, GROUP>) new DefaultTaskIdAdapter();
    protected boolean          autoStart                   = true;
    private boolean            enableFutureTracking        = true;
    private long               maximumFutureWaitTime = TimeUnit.MINUTES.toMillis(60);
    
    private boolean            asyncronousTaskDistribution = false;
    private int                asyncronousTaskDistributionQueueSize = 500;
    private long               recoveryProcessPollInterval = 30000;
    
    private ExecutorLoadBalancingConfig<GROUP> executorLoadBalancingConfig = new ExecutorLoadBalancingConfig<GROUP>();
    
    /**
     * Please consider using one of the ExecutorConfigs static factory methods
     */
    public ExecutorConfig() {
        
    }

    /**
     * This setting will make HazelTask buffer task distributions in a bounded queue
     * before they are sent off to the worker nodes.  This helps you get around slowness
     * in worker nodes due to GC pauses, or other load that may negatively impact 
     * Hazelcast's ability to handle calls.
     * 
     * NOTE: I think hazelcast might already actually do this???  Still testing... I will 
     * remove this code if hazelcast does do this behind the scenes.
     * 
     * @return
     */
    @Deprecated
    public ExecutorConfig<GROUP> useAsyncronousTaskDistribution() {
        asyncronousTaskDistribution = true;
        return this;
    }
    
    /**
     * When using asyncronousTaskDistribution you may define how many tasks are allowed 
     * to buffer before it start blocking the calling thread.  If a worker is JVM thrashing
     * and hazelcast is having trouble talking to it, this buffer will fill up.  This is 
     * meant to protect against JVM pauses.  You really don't want the buffer to be infinite
     * in case things go bad in your cluster.  You just want it to be big enough to get around
     * occasional JVM pauses due to GC.
     * 
     * NOTE: I think hazelcast might already actually do this???  Still testing... I will 
     * remove this code if hazelcast does do this behind the scenes.
     * 
     * @see useAsyncronousTaskDistribution()
     * @param queueSize
     * @return
     */
    @Deprecated
    public ExecutorConfig<GROUP> withAsyncronousTaskDistributionQueueSize(int queueSize) {
        this.asyncronousTaskDistributionQueueSize = queueSize;
        return this;
    }

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
        
        //for now we will have a fixed thread pool size
        this.maxPoolSize = threadCount;
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
     * By default the longest we will track a future is 60 minutes.  If partition data is 
     * lost, we try to figure out which futures were possible affected and error them.  This
     * setting is a fallback in case we miss erroring a future and ensures it will be cleaned up.
     * 
     * Futures waiting for longer than this time will be errored with a TimeoutException
     * 
     * @param millis
     * @return
     */
    public ExecutorConfig<GROUP> withMaximumFutureWaitTime(long millis) {
        this.maximumFutureWaitTime = millis;
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
    
    public ExecutorConfig<GROUP> withMaxThreadKeepAliveTime(long tti) {
        this.maxThreadKeepAlive = tti;
        return this;
    }
    
    public long getMaxThreadKeepAliveTime() {
        return maxThreadKeepAlive;
    }
    
    public ExecutorConfig<GROUP> withLoadBalancingConfig(ExecutorLoadBalancingConfig<GROUP> config) {
        this.executorLoadBalancingConfig = config;
        return this;
    }
    
    public ExecutorLoadBalancingConfig<GROUP> getLoadBalancingConfig() {
        return this.executorLoadBalancingConfig;
    }

    public boolean isAsyncronousTaskDistribution() {
        return asyncronousTaskDistribution;
    }

    public int getAsyncronousTaskDistributionQueueSize() {
        return asyncronousTaskDistributionQueueSize;
    }

    public long getMaximumFutureWaitTime() {
        return maximumFutureWaitTime;
    }
}
