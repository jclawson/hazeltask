package com.hazeltask.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import com.hazeltask.HazeltaskService;
import com.hazeltask.HazeltaskServiceListener;
import com.hazeltask.HazeltaskTopology;
import com.hazeltask.batch.TaskBatchingService;
import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.core.concurrent.collections.router.ListRouter;
import com.succinctllc.hazelcast.work.HazelcastWork;
import com.succinctllc.hazelcast.work.WorkId;

public class DistributedExecutorService implements ExecutorService, HazeltaskService<DistributedExecutorService> {

    private ExecutorConfig executorConfig;
    private final HazeltaskTopology        topology;
    private volatile boolean               isReady = false;
    private final ListRouter<Member>       memberRouter;
    private final LocalTaskExecutorService localExecutorService;
    private final WorkIdAdapter            workIdAdapter;
    private final DistributedFutureTracker futureTracker;
    private CopyOnWriteArrayList<HazeltaskServiceListener<DistributedExecutorService>> listeners = new CopyOnWriteArrayList<HazeltaskServiceListener<DistributedExecutorService>>();
    private final ILogger LOGGER;
    
    //max number of times to try and submit a work before giving up
    private final int MAX_SUBMIT_TRIES = 10;
    
    public DistributedExecutorService(HazeltaskTopology hcTopology, ExecutorConfig executorConfig, DistributedFutureTracker futureTracker) {
        this.topology = hcTopology;
        this.executorConfig = executorConfig;
        this.memberRouter = executorConfig.getMemberRouterFactory().createRouter(new Callable<List<Member>>(){
            public List<Member> call() throws Exception {
                return topology.getReadyMembers();
            }
        });
        
        workIdAdapter = executorConfig.getWorkIdAdapter();
        
        this.futureTracker = futureTracker;
        
        this.localExecutorService = new LocalTaskExecutorService(topology, executorConfig);
        
        LOGGER = topology.getLoggingService().getLogger(DistributedExecutorService.class.getName());
    }

    public ExecutorConfig getExecutorConfig() {
        return this.executorConfig;
    }

    public void execute(Runnable command) {
        submitHazelcastWork(createHazelcastWorkWrapper(command), false);
    }
    
    

    public void shutdown() {
        topology.getTopologyService().shutdown();
    }

    public List<Runnable> shutdownNow() {
        return new ArrayList<Runnable>(topology.getTopologyService().shutdownNow());
    }
    
    protected void doShutdown() {
        doShutdownNow();
    }
    
    protected List<HazelcastWork> doShutdownNow() {
        for(HazeltaskServiceListener<DistributedExecutorService> listener : listeners)
            listener.onBeginShutdown(this);
        
        //FIXME implement this
        
        
        for(HazeltaskServiceListener<DistributedExecutorService> listener : listeners)
            listener.onEndShutdown(this);
        
        return Collections.emptyList();
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
        if(futureTracker == null)
            throw new IllegalStateException("FutureTracker is null");
        
        HazelcastWork work = createHazelcastWorkWrapper(task);
        DistributedFuture<T> future = futureTracker.createFuture(work);
        if(!submitHazelcastWork(work, false)) {
            //remove future from tracker, error out future with duplicate exception
            //i hate this... it would be a cool feature to attach this future to the 
            //work in progress.  its easier to just cancel it for now
            //TODO: should we cancel or throw an exception?
            //  - i think cancel, because presumably the work is going to run we just can't track it
            //    so if you don't care about the result, no harm
            future.setCancelled();
            futureTracker.removeAll(work.getUniqueIdentifier());
        }
        return future;
    }
    
    private HazelcastWork createHazelcastWorkWrapper(Runnable task){
        if(task instanceof HazelcastWork) {
            ((HazelcastWork) task).updateCreatedTime();
            return (HazelcastWork) task;
        } else {
            return new HazelcastWork(topology.getName(), workIdAdapter.createWorkId(task), task);
        }
    }
    
    private HazelcastWork createHazelcastWorkWrapper(Callable<?> task) {
        return new HazelcastWork(topology.getName(), workIdAdapter.createWorkId(task), task);
    }

    public <T> Future<T> submit(Runnable task, T result) {
        throw new RuntimeException("Not Implemented Yet");
    }

    public Future<?> submit(Runnable task) {
        if(futureTracker == null)
            throw new IllegalStateException("FutureTracker is null");
        
        HazelcastWork work = createHazelcastWorkWrapper(task);
        DistributedFuture<?> future = futureTracker.createFuture(work);
        submitHazelcastWork(work, false);
        return future;
    }
    
    protected boolean submitHazelcastWork(HazelcastWork wrapper, boolean isResubmitting) {      
        WorkId workKey = wrapper.getWorkId();
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
                topology.getTopologyService().addPendingTask(wrapper, true);
            } else {
                executeTask = topology.getTopologyService().addPendingTask(wrapper, false);
            }
            
            if(executeTask) {
                Member m = memberRouter.next();
                if(m == null) {
                    LOGGER.log(Level.WARNING, "Work submitted to writeAheadLog but no members are online to do the work.");
                    return false;
                }
                
                if(topology.getTopologyService().sendTask(wrapper, m, executorConfig.isAcknowlegeWorkSubmission())) {
                    return true;
                } else {
                    isResubmitting = true;
                }
            } else {
                //do not submit
                return false;
            }
        }
        
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
        
        localExecutorService.start();
        //FIXME: startup
        
        for(HazeltaskServiceListener<DistributedExecutorService> listener : listeners)
            listener.onEndStart(this);
    }

    public void addServiceListener(HazeltaskServiceListener<DistributedExecutorService> listener) {
        this.listeners.add(listener);
    }
    
    public void addListener(ExecutorListener listener) {
        this.localExecutorService.addListener(listener);
    }

}
