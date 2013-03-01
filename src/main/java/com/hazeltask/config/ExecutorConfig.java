package com.hazeltask.config;

import java.io.Serializable;
import java.util.concurrent.ThreadFactory;

import com.hazeltask.executor.task.DefaultTaskIdAdapter;
import com.hazeltask.executor.task.TaskIdAdapter;

public class ExecutorConfig<ID extends Serializable, GROUP extends Serializable> {
    protected boolean          acknowlegeTaskSubmission = false;
    protected boolean          disableWorkers           = false;
    protected int              threadCount              = 4;

    protected TaskIdAdapter<?, ID, GROUP>    taskIdAdapter = (TaskIdAdapter<?, ID, GROUP>) new DefaultTaskIdAdapter();
    protected boolean          autoStart                = true;
    private boolean            enableFutureTracking     = true;
    
    private ThreadFactory threadFactory = null;
    
    private ExecutorLoadBalancingConfig<ID,GROUP> executorLoadBalancingConfig = new ExecutorLoadBalancingConfig<ID, GROUP>();
    
    /**
     * Please use the ExecutorConfigs factory
     */
    public ExecutorConfig() {
        
    }

    public ExecutorConfig<ID, GROUP> withAcknowlegeTaskSubmission(boolean acknowlegeTaskSubmission) {
        this.acknowlegeTaskSubmission = acknowlegeTaskSubmission;
        return this;
    }

    public ExecutorConfig<ID, GROUP> acknowlegeTaskSubmission() {
        this.acknowlegeTaskSubmission = true;
        return this;
    }

    public ExecutorConfig<ID, GROUP> withDisableWorkers(boolean disableWorkers) {
        this.disableWorkers = disableWorkers;
        return this;
    }

    public ExecutorConfig<ID, GROUP> disableWorkers() {
        this.disableWorkers = true;
        return this;
    }
    
    public ExecutorConfig<ID, GROUP> disableFutureSupport() {
        this.enableFutureTracking = false;
        return this;
    }

    public boolean isFutureSupportEnabled() {
        return this.enableFutureTracking;
    }

    public ExecutorConfig<ID, GROUP> withThreadCount(int threadCount) {
        this.threadCount = threadCount;
        return this;
    }

    public ExecutorConfig<ID, GROUP> withTaskIdAdapter(TaskIdAdapter<?,ID,GROUP> taskIdAdapter) {
        this.taskIdAdapter = taskIdAdapter;
        return this;
    }

    /**
     * By default we will automatically startup the task system when its
     * created. Some developers may want to delay the startup, and handle in
     * manually. For example, waiting until the web server has fully started.
     * 
     * @param autoStart
     * @return
     */
    public ExecutorConfig<ID, GROUP> withAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
        return this;
    }

    /**
     * @see withAutoStart
     */
    public ExecutorConfig<ID, GROUP> disableAutoStart() {
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
        return threadCount;
    }
  
    public TaskIdAdapter<?, ID, GROUP> getTaskIdAdapter() {
        return taskIdAdapter;
    }
 
    public boolean isAutoStart() {
        return autoStart;
    }
    
    public ExecutorConfig<ID, GROUP> withThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
        return this;
    }
     
    public ThreadFactory getThreadFactory() {
        return this.threadFactory;
    }
    
    public ExecutorConfig<ID,GROUP> withLoadBalancingConfig(ExecutorLoadBalancingConfig<ID,GROUP> config) {
        this.executorLoadBalancingConfig = config;
        return this;
    }
    
    public ExecutorLoadBalancingConfig<ID,GROUP> getLoadBalancingConfig() {
        return this.executorLoadBalancingConfig;
    }
}
