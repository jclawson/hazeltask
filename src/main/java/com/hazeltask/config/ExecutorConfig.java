package com.hazeltask.config;

import static java.util.concurrent.TimeUnit.MINUTES;

import com.hazeltask.core.concurrent.collections.router.ListRouterFactory;
import com.hazeltask.core.concurrent.collections.router.RoundRobinRouter;
import com.hazeltask.executor.task.DefaultWorkIdAdapter;
import com.hazeltask.executor.task.WorkIdAdapter;

public class ExecutorConfig {
    protected boolean          acknowlegeWorkSubmission = false;
    protected boolean          disableWorkers           = false;
    protected int              threadCount              = 4;
    @SuppressWarnings("rawtypes")
    protected WorkIdAdapter    workIdAdapter            = new DefaultWorkIdAdapter();
    protected boolean          autoStart                = true;
    private ListRouterFactory  memberRouterFactory      = RoundRobinRouter.FACTORY;
    private boolean            enableFutureTracking     = true;
    private long               rebalanceTaskPeriod      = MINUTES.toMillis(2);

    // TODO: support autoStartDelay
    // protected long autoStartDelay = 0;
    // TODO: Allow developers to provide all the threads we need

    public ExecutorConfig withAcknowlegeWorkSubmission(boolean acknowlegeWorkSubmission) {
        this.acknowlegeWorkSubmission = acknowlegeWorkSubmission;
        return this;
    }

    public ExecutorConfig acknowlegeWorkSubmission() {
        this.acknowlegeWorkSubmission = true;
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
    public ExecutorConfig withWorkIdAdapter(WorkIdAdapter workIdAdapter) {
        this.workIdAdapter = workIdAdapter;
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

    public boolean isAcknowlegeWorkSubmission() {
        return acknowlegeWorkSubmission;
    }

    public boolean isDisableWorkers() {
        return disableWorkers;
    }

    public int getThreadCount() {
        return threadCount;
    }

    @SuppressWarnings("rawtypes")
    public WorkIdAdapter getWorkIdAdapter() {
        return workIdAdapter;
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
