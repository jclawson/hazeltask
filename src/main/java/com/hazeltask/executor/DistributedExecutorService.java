package com.hazeltask.executor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazeltask.HazeltaskServiceListener;
import com.hazeltask.HazeltaskTopology;
import com.hazeltask.ServiceListenable;
import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.core.concurrent.collections.router.ListRouter;
import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.task.HazeltaskTask;
import com.hazeltask.executor.task.TaskIdAdapter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.TimerContext;

public class DistributedExecutorService implements ExecutorService, ServiceListenable<DistributedExecutorService> {

    private ExecutorConfig executorConfig;
    private final HazeltaskTopology        topology;
    private final ListRouter<Member>       memberRouter;
    private final LocalTaskExecutorService localExecutorService;
    
    @SuppressWarnings("rawtypes")
    private final TaskIdAdapter            taskIdAdapter;
    private final DistributedFutureTracker futureTracker;
    private CopyOnWriteArrayList<HazeltaskServiceListener<DistributedExecutorService>> listeners = new CopyOnWriteArrayList<HazeltaskServiceListener<DistributedExecutorService>>();
    private final ILogger LOGGER;
    private final IExecutorTopologyService  executorTopologyService;
    
    private com.yammer.metrics.core.Timer taskAddedTimer;
    private Meter tasksRejected;
    
    //max number of times to try and submit a work before giving up
    private final int MAX_SUBMIT_TRIES = 10;
    
    public DistributedExecutorService(HazeltaskTopology         hcTopology, 
                                      IExecutorTopologyService  executorTopologyService, 
                                      ExecutorConfig            executorConfig, 
                                      DistributedFutureTracker  futureTracker, 
                                      LocalTaskExecutorService localExecutorService) {
        this.topology = hcTopology;
        this.executorConfig = executorConfig;
        this.executorTopologyService = executorTopologyService;
        
        this.memberRouter = executorConfig.getMemberRouterFactory().createRouter(new Callable<List<Member>>(){
            public List<Member> call() throws Exception {
                return topology.getReadyMembers();
            }
        });
        
        taskIdAdapter = executorConfig.getTaskIdAdapter();
        
        this.futureTracker = futureTracker;
        
        this.localExecutorService = localExecutorService;
        
        LOGGER = topology.getLoggingService().getLogger(DistributedExecutorService.class.getName());
        
        taskAddedTimer = hcTopology.getExecutorMetrics().getTaskSubmitTimer().getMetric();
        tasksRejected = hcTopology.getExecutorMetrics().getTaskRejectedMeter().getMetric();
    }

    public ExecutorConfig getExecutorConfig() {
        return this.executorConfig;
    }

    public void execute(Runnable command) {
        TimerContext ctx = taskAddedTimer.time();
        try {
            submitHazeltaskTask(createHazeltaskTaskWrapper(command), false);
        } finally {
            ctx.stop();
        }
    }
    
    

    public void shutdown() {
        topology.getTopologyService().shutdown();
    }

    public List<Runnable> shutdownNow() {
        return new ArrayList<Runnable>(topology.getTopologyService().shutdownNow());
    }
    
    protected void doShutdown() {
        doShutdownNow(false);
    }
    
    protected List<HazeltaskTask> doShutdownNow() {
        return doShutdownNow(true);
    }
    
    protected List<HazeltaskTask> doShutdownNow(boolean shutdownNow) {
        for(HazeltaskServiceListener<DistributedExecutorService> listener : listeners)
            listener.onBeginShutdown(this);
        
        //TODO: is everything shutdown?
        List<HazeltaskTask> tasks = null;
        if(!executorConfig.isDisableWorkers()) {
            if(shutdownNow)
                tasks = this.localExecutorService.shutdownNow();
            else
                this.localExecutorService.shutdown();
        }
        
        for(HazeltaskServiceListener<DistributedExecutorService> listener : listeners)
            listener.onEndShutdown(this);
        
        return tasks;
    }

    public boolean isShutdown() {
        // TODO Auto-generated method stub
        throw new RuntimeException("Not Implemented Yet");
    }

    public boolean isTerminated() {
        // TODO Auto-generated method stub
        throw new RuntimeException("Not Implemented Yet");
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        // TODO Auto-generated method stub
        throw new RuntimeException("Not Implemented Yet");
    }

    public <T> Future<T> submit(Callable<T> task) {
        TimerContext ctx = taskAddedTimer.time();
        try {
            if(futureTracker == null)
                throw new IllegalStateException("FutureTracker is null");
            
            HazeltaskTask taskWrapper = createHazeltaskTaskWrapper(task);
            DistributedFuture<T> future = futureTracker.createFuture(taskWrapper);
            if(!submitHazeltaskTask(taskWrapper, false)) {
                //remove future from tracker, error out future with duplicate exception
                //i hate this... it would be a cool feature to attach this future to the 
                //work in progress.  its easier to just cancel it for now
                //TODO: should we cancel or throw an exception?
                //  - i think cancel, because presumably the work is going to run we just can't track it
                //    so if you don't care about the result, no harm
                LOGGER.log(Level.SEVERE, "Unable to submit HazeltaskTask to worker member");
                future.setCancelled();
                futureTracker.removeAll(taskWrapper.getId());
            }
            return future;
        } finally {
            ctx.stop();
        }
    }
    
    @SuppressWarnings("unchecked")
    private HazeltaskTask createHazeltaskTaskWrapper(Runnable task){
        if(task instanceof HazeltaskTask) {
            ((HazeltaskTask) task).updateCreatedTime();
            return (HazeltaskTask) task;
        } else {
            return new HazeltaskTask(topology.getName(), 
                                     taskIdAdapter.getTaskId(task), 
                                     taskIdAdapter.getTaskGroup(task), 
                                     task);
        }
    }
    
    @SuppressWarnings("unchecked")
    private HazeltaskTask createHazeltaskTaskWrapper(Callable<?> task) {
        return new HazeltaskTask(topology.getName(), 
                                 taskIdAdapter.getTaskId(task), 
                                 taskIdAdapter.getTaskGroup(task), 
                                 task);
    }

    public <T> Future<T> submit(Runnable task, T result) {
        TimerContext ctx = taskAddedTimer.time();
        try {
            throw new RuntimeException("Not Implemented Yet");
        } finally {
            ctx.stop();
        }   
    }

    public Future<?> submit(Runnable task) {
        TimerContext ctx = taskAddedTimer.time();
        try {
            if(futureTracker == null)
                throw new IllegalStateException("FutureTracker is null");
            
            HazeltaskTask taskWrapper = createHazeltaskTaskWrapper(task);
            DistributedFuture<?> future = futureTracker.createFuture(taskWrapper);
            submitHazeltaskTask(taskWrapper, false);
            return future;
        } finally {
            ctx.stop();
        }
    }
    
    protected boolean submitHazeltaskTask(HazeltaskTask wrapper, boolean isResubmitting) {      
        
        //WorkId workKey = wrapper.getWorkId();
        boolean executeTask = true;
        /*
         * with acknowledgeWorkSubmition, we will sit in this loop until a 
         * node accepts our work item.  Currently, a node will accept as long as
         * a MemberLeftException is not thrown
         */
        int tries = 0;
        while(++tries <= MAX_SUBMIT_TRIES) {
            if(isResubmitting) {
                wrapper.setSubmissionCount(wrapper.getSubmissionCount()+1);
                executorTopologyService.addPendingTask(wrapper, true);
            } else {
                executeTask = executorTopologyService.addPendingTask(wrapper, false);
            }
            
            if(executeTask) {
                Member m = memberRouter.next();
                if(m == null) {
                    LOGGER.log(Level.WARNING, "Work submitted to writeAheadLog but no members are online to do the work.");
                    tasksRejected.mark();
                    return false;
                }
                
                try {
                    if(executorTopologyService.sendTask(wrapper, m, executorConfig.isAcknowlegeTaskSubmission())) {
                        return true;
                    } else {
                        isResubmitting = true;
                    }
                } catch (TimeoutException e) {
                    LOGGER.log(Level.WARNING, "Timed out while trying to submit task for try #"+tries+", trying again...");
                    isResubmitting = true;
                }
            } else {
                //do not submit
                tasksRejected.mark();
                return false;
            }
        }
        
        tasksRejected.mark();
        throw new RuntimeException("Unable to submit work to nodes. I tried "+MAX_SUBMIT_TRIES+" times.");
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        // TODO Auto-generated method stub
        throw new RuntimeException("Not Implemented Yet");
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
            TimeUnit unit) throws InterruptedException {
        // TODO Auto-generated method stub
        throw new RuntimeException("Not Implemented Yet");
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException,
            ExecutionException {
        // TODO Auto-generated method stub
        throw new RuntimeException("Not Implemented Yet");
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        // TODO Auto-generated method stub
        throw new RuntimeException("Not Implemented Yet");
    }

    public void startup() {
        for(HazeltaskServiceListener<DistributedExecutorService> listener : listeners)
            listener.onBeginStart(this);
        
        if(!executorConfig.isDisableWorkers())
            localExecutorService.startup();
        
        for(HazeltaskServiceListener<DistributedExecutorService> listener : listeners)
            listener.onEndStart(this);
    }

    public void addServiceListener(HazeltaskServiceListener<DistributedExecutorService> listener) {
        this.listeners.add(listener);
    }
    
    public void addListener(ExecutorListener listener) {
        if(!executorConfig.isDisableWorkers())
            this.localExecutorService.addListener(listener);
    }
    
    public <ID extends Serializable, GROUP extends Serializable> LocalTaskExecutorService<ID,GROUP> getLocalTaskExecutorService() {
        return this.localExecutorService;
    }

}
