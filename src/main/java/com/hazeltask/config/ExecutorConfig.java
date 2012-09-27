package com.hazeltask.config;

import static java.util.concurrent.TimeUnit.MINUTES;

import com.hazeltask.core.concurrent.collections.router.ListRouterFactory;
import com.hazeltask.core.concurrent.collections.router.RoundRobinRouter;
import com.hazeltask.executor.task.DefaultTaskIdAdapter;
import com.hazeltask.executor.task.TaskIdAdapter;

public class ExecutorConfig {
    protected boolean          acknowlegeTaskSubmission = false;
    protected boolean          disableWorkers           = false;
    protected int              threadCount              = 4;
    @SuppressWarnings("rawtypes")
    protected TaskIdAdapter    taskIdAdapter            = new DefaultTaskIdAdapter();
    protected boolean          autoStart                = true;
    private ListRouterFactory  memberRouterFactory      = RoundRobinRouter.FACTORY;
    private boolean            enableFutureTracking     = true;
    private long               rebalanceTaskPeriod      = MINUTES.toMillis(2);

    // TODO: support autoStartDelay
    // protected long autoStartDelay = 0;
    // TODO: Allow developers to provide all the threads we need

    public ExecutorConfig withAcknowlegeTaskSubmission(boolean acknowlegeTaskSubmission) {
        this.acknowlegeTaskSubmission = acknowlegeTaskSubmission;
        return this;
    }

    public ExecutorConfig acknowlegeTaskSubmission() {
        this.acknowlegeTaskSubmission = true;
        return this;
    }

    public ExecutorConfig withDisableWorkers(boolean disableWorkers) {
        this.disableWorkers = disableWorkers;
        return this;
    }

    public ExecutorConfig disableWorkers() {
        this.disableWorkers = true;
        return this;
    }
    
    public ExecutorConfig disableFutureSupport() {
        this.enableFutureTracking = false;
        return this;
    }
    
    public boolean isFutureSupportEnabled() {
        return this.enableFutureTracking;
    }

    public ExecutorConfig withThreadCount(int threadCount) {
        this.threadCount = threadCount;
        return this;
    }

    @SuppressWarnings("rawtypes")
    public ExecutorConfig withTaskIdAdapter(TaskIdAdapter taskIdAdapter) {
        this.taskIdAdapter = taskIdAdapter;
        return this;
    }
    
    public ExecutorConfig withMemberRouterFactory(ListRouterFactory factory) {
        this.memberRouterFactory = factory;
        return this;
    }
    
    public ListRouterFactory getMemberRouterFactory() {
        return this.memberRouterFactory;
    }

    /**
     * By default we will automatically startup the task system when its
     * created. Some developers may want to delay the startup, and handle in
     * manually. For example, waiting until the web server has fully started.
     * 
     * @param autoStart
     * @return
     */
    public ExecutorConfig withAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
        return this;
    }

    /**
     * @see withAutoStart
     */
    public ExecutorConfig disableAutoStart() {
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

    @SuppressWarnings("rawtypes")
    public TaskIdAdapter getTaskIdAdapter() {
        return taskIdAdapter;
    }

    public boolean isAutoStart() {
        return autoStart;
    }
    
    public void withRebalanceTaskPeriod(long rebalanceTaskPeriod) {
        this.rebalanceTaskPeriod = rebalanceTaskPeriod;
    }
    
    public long getRebalanceTaskPeriod() {
        return this.rebalanceTaskPeriod;
    }
}
