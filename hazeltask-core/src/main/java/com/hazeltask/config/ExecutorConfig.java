package com.hazeltask.config;

import java.io.Serializable;
import java.util.concurrent.ThreadFactory;

import com.hazeltask.executor.task.DefaultTaskIdAdapter;
import com.hazeltask.executor.task.TaskIdAdapter;

public class ExecutorConfig<GROUP extends Serializable> {
    protected boolean          acknowlegeTaskSubmission = false;
    protected boolean          disableWorkers           = false;
    protected int              corePoolSize             = 4;
    protected int              maxPoolSize              = 4;
    protected long             maxThreadKeepAlive       = 60000;

    @SuppressWarnings("unchecked")
    protected TaskIdAdapter<?, GROUP>    taskIdAdapter = (TaskIdAdapter<?, GROUP>) new DefaultTaskIdAdapter();
    protected boolean          autoStart                = true;
    private boolean            enableFutureTracking     = true;
    
    private long               recoveryProcessPollInterval = 30000;
    
    private ThreadFactory threadFactory = null;
    
    private ExecutorLoadBalancingConfig<GROUP> executorLoadBalancingConfig = new ExecutorLoadBalancingConfig<GROUP>();
    
    /**
     * Please use the ExecutorConfigs factory
     */
    public ExecutorConfig() {
        
    }

    public ExecutorConfig<GROUP> withAcknowlegeTaskSubmission(boolean acknowlegeTaskSubmission) {
        this.acknowlegeTaskSubmission = acknowlegeTaskSubmission;
        return this;
    }

    public ExecutorConfig<GROUP> acknowlegeTaskSubmission() {
        this.acknowlegeTaskSubmission = true;
        return this;
    }

    public ExecutorConfig<GROUP> withDisableWorkers(boolean disableWorkers) {
        this.disableWorkers = disableWorkers;
        return this;
    }

    public ExecutorConfig<GROUP> disableWorkers() {
        this.disableWorkers = true;
        return this;
    }
    
    public ExecutorConfig<GROUP> disableFutureSupport() {
        this.enableFutureTracking = false;
        return this;
    }

    public boolean isFutureSupportEnabled() {
        return this.enableFutureTracking;
    }

    public ExecutorConfig<GROUP> withThreadCount(int threadCount) {
        this.corePoolSize = threadCount;
        return this;
    }

    public ExecutorConfig<GROUP> withTaskIdAdapter(TaskIdAdapter<?,GROUP> taskIdAdapter) {
        this.taskIdAdapter = taskIdAdapter;
        return this;
    }
    
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
 
    public boolean isAcknowlegeTaskSubmission() {
        return acknowlegeTaskSubmission;
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
    
    public ExecutorConfig<GROUP> withThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
        return this;
    }
     
    public ThreadFactory getThreadFactory() {
        return this.threadFactory;
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
