package com.hazeltask.config;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.io.Serializable;
import java.util.Map.Entry;
import java.util.concurrent.ThreadFactory;

import com.hazelcast.core.Member;
import com.hazeltask.config.helpers.AbstractTaskRouterFactory;
import com.hazeltask.core.concurrent.collections.router.ListRouterFactory;
import com.hazeltask.core.concurrent.collections.router.RoundRobinRouter;
import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;
import com.hazeltask.executor.task.DefaultTaskIdAdapter;
import com.hazeltask.executor.task.HazeltaskTask;
import com.hazeltask.executor.task.TaskIdAdapter;

public class ExecutorConfig<ID extends Serializable, GROUP extends Serializable> {
    protected boolean          acknowlegeTaskSubmission = false;
    protected boolean          disableWorkers           = false;
    protected int              threadCount              = 4;
    @SuppressWarnings("rawtypes")
    protected TaskIdAdapter    taskIdAdapter            = new DefaultTaskIdAdapter();
    protected boolean          autoStart                = true;
    private ListRouterFactory<Member>  memberRouterFactory      = RoundRobinRouter.newFactory();
    private boolean            enableFutureTracking     = true;
    private long               rebalanceTaskPeriod      = MINUTES.toMillis(2);
    private ListRouterFactory<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID,GROUP>>>>  taskRouterFactory        = RoundRobinRouter.newFactory();
    private ThreadFactory threadFactory = null;
    
    // TODO: support autoStartDelay
    // protected long autoStartDelay = 0;
    // TODO: Allow developers to provide all the threads we need

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
    
    public ExecutorConfig<ID, GROUP> withMemberRouterFactory(ListRouterFactory<Member> factory) {
        this.memberRouterFactory = factory;
        return this;
    }
    
    public ListRouterFactory<Member> getMemberRouterFactory() {
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

    @SuppressWarnings("rawtypes")
    public TaskIdAdapter getTaskIdAdapter() {
        return taskIdAdapter;
    }

    public boolean isAutoStart() {
        return autoStart;
    }
    
    public ExecutorConfig<ID, GROUP> withRebalanceTaskPeriod(long rebalanceTaskPeriod) {
        this.rebalanceTaskPeriod = rebalanceTaskPeriod;
        return this;
    }
    
    public long getRebalanceTaskPeriod() {
        return this.rebalanceTaskPeriod;
    }
    
    public ExecutorConfig<ID, GROUP> withTaskRouterFactory(AbstractTaskRouterFactory<ID, GROUP> router) {
        this.taskRouterFactory = (ListRouterFactory<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID,GROUP>>>>) router;
        return this;
    }
    
    public ListRouterFactory<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID,GROUP>>>> getTaskRouterFactory() {
        return (ListRouterFactory<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID,GROUP>>>>) this.taskRouterFactory;
    }
    
    public ExecutorConfig<ID, GROUP> withThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
        return this;
    }
    
    public ThreadFactory getThreadFactory() {
        return this.threadFactory;
    }
}
