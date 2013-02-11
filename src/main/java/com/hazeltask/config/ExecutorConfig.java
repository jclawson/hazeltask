package com.hazeltask.config;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.Map.Entry;
import java.util.concurrent.ThreadFactory;

import com.hazelcast.core.Member;
import com.hazeltask.core.concurrent.collections.grouped.Groupable;
import com.hazeltask.core.concurrent.collections.router.ListRouterFactory;
import com.hazeltask.core.concurrent.collections.router.RoundRobinRouter;
import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;
import com.hazeltask.executor.task.DefaultTaskIdAdapter;
import com.hazeltask.executor.task.HazeltaskTask;
import com.hazeltask.executor.task.TaskIdAdapter;

public class ExecutorConfig {
    protected boolean          acknowlegeTaskSubmission = false;
    protected boolean          disableWorkers           = false;
    protected int              threadCount              = 4;
    @SuppressWarnings("rawtypes")
    protected TaskIdAdapter    taskIdAdapter            = new DefaultTaskIdAdapter();
    protected boolean          autoStart                = true;
    private ListRouterFactory<Member>  memberRouterFactory      = RoundRobinRouter.newFactory();
    private boolean            enableFutureTracking     = true;
    private long               rebalanceTaskPeriod      = MINUTES.toMillis(2);
    private ListRouterFactory<Entry<String, ITrackedQueue<HazeltaskTask>>>  taskRouterFactory        = RoundRobinRouter.newFactory();
    private ThreadFactory threadFactory = null;
    
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
    
    public ExecutorConfig withMemberRouterFactory(ListRouterFactory<Member> factory) {
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
    
    public ExecutorConfig withRebalanceTaskPeriod(long rebalanceTaskPeriod) {
        this.rebalanceTaskPeriod = rebalanceTaskPeriod;
        return this;
    }
    
    public long getRebalanceTaskPeriod() {
        return this.rebalanceTaskPeriod;
    }
    
    public <E extends Groupable> ExecutorConfig withTaskRouterFactory(ListRouterFactory<Entry<String, ITrackedQueue<HazeltaskTask>>> router) {
        this.taskRouterFactory = router;
        return this;
    }
    
    public ListRouterFactory<Entry<String, ITrackedQueue<HazeltaskTask>>> getTaskRouterFactory() {
        return this.taskRouterFactory;
    }
    
    public ExecutorConfig withThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
        return this;
    }
    
    public ThreadFactory getThreadFactory() {
        return this.threadFactory;
    }
}
