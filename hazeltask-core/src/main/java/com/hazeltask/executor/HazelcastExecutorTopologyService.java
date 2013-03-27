package com.hazeltask.executor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MessageListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.SqlPredicate;
import com.hazeltask.HazeltaskTopology;
import com.hazeltask.clusterop.GetLocalGroupQueueSizesOp;
import com.hazeltask.clusterop.GetLocalQueueSizesOp;
import com.hazeltask.clusterop.GetOldestTimestampOp;
import com.hazeltask.clusterop.StealTasksOp;
import com.hazeltask.clusterop.SubmitTaskOp;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.executor.task.HazeltaskTask;
import com.hazeltask.executor.task.TaskResponse;
import com.hazeltask.hazelcast.MemberTasks;
import com.hazeltask.hazelcast.MemberTasks.MemberResponse;
import com.hazeltask.hazelcast.MemberValuePair;

public class HazelcastExecutorTopologyService<GROUP extends Serializable> implements IExecutorTopologyService<GROUP> {
    //private final BloomFilter<CharSequence> bloomFilter;
    private HazeltaskTopology<GROUP> topology;
    private String topologyName;
    private final Member me;
    private static ILogger LOGGER = Logger.getLogger(HazelcastExecutorTopologyService.class.getName());
    
    
    private final ExecutorService communicationExecutorService;

    private final ExecutorService taskDistributor;
    //private final CopyOnWriteArrayListSet<Member> readyMembers;
    private final IMap<UUID, HazeltaskTask<GROUP>>                            pendingTask;
    private final ILock rebalanceTasksLock;
    private final ITopic<TaskResponse<Serializable>>      taskResponseTopic;
    private final HazelcastInstance hazelcast;
    
    private final Executor asyncTaskDistributorExecutor;
    
    public HazelcastExecutorTopologyService(HazeltaskConfig<GROUP> hazeltaskConfig, HazeltaskTopology<GROUP> topology) {
        com.hazeltask.config.ExecutorConfig<GROUP> executorConfig = hazeltaskConfig.getExecutorConfig();
        topologyName = hazeltaskConfig.getTopologyName();
        this.topology = topology;
        hazelcast = hazeltaskConfig.getHazelcast();
        this.me = hazelcast.getCluster().getLocalMember();
        communicationExecutorService = hazelcast.getExecutorService(name("com"));
        
        String taskDistributorName = name("task-distributor");
        
        //limit the threads on the distributor to 1 thread
        hazelcast.getConfig()
            .addExecutorConfig(new ExecutorConfig()
                .setName(taskDistributorName)
                .setMaxPoolSize(1)
                .setCorePoolSize(1)
            );
        
        taskDistributor =  hazelcast.getExecutorService(taskDistributorName);
        //readyMembers = new CopyOnWriteArrayListSet<Member>();
        
        if(hazeltaskConfig.getExecutorConfig().isAsyncronousTaskDistribution())
            asyncTaskDistributorExecutor =  new ThreadPoolExecutor(1, 1,
                            0L, TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<Runnable>(executorConfig.getAsyncronousTaskDistributionQueueSize()),
                            hazeltaskConfig.getThreadFactory().named("async"),
                            new CallerRunsPolicy());
        else
            asyncTaskDistributorExecutor = null;
        
        String pendingTaskMapName = name("pending-tasks");
        hazelcast.getConfig()
        .addMapConfig(new MapConfig()
            .setName(taskDistributorName)
            .addMapIndexConfig(new MapIndexConfig("createdAtMillis", false)));
        
        pendingTask = hazelcast.getMap(pendingTaskMapName);
        taskResponseTopic = hazelcast.getTopic(name("task-response"));
        
        rebalanceTasksLock = hazelcast.getLock(name("task-balance"));
    }
    
    private String name(String name) {
        return topologyName + "-" + name;
    }
    
//    public boolean isMemberReady(Member member) {
//        // TODO Auto-generated method stub
//        return false;
//    }
    
    public void sendTask(HazeltaskTask<GROUP> task, Member member) throws TimeoutException {
        DistributedTask<Boolean> distTask = MemberTasks.create(new SubmitTaskOp<GROUP>(task, topologyName), member);
        if(asyncTaskDistributorExecutor != null) {
            asyncTaskDistributorExecutor.execute(new $SendTaskToWorker(distTask, taskDistributor));
        } else {
            taskDistributor.execute(distTask);
        }
    }
    
    private static class $SendTaskToWorker implements Runnable {
        private final ExecutorService taskDistributor;
        private final DistributedTask<Boolean> task;
        
        private $SendTaskToWorker(DistributedTask<Boolean> task, ExecutorService taskDistributor) {
            this.task = task;
            this.taskDistributor = taskDistributor;
        }
        
        @Override
        public void run() {
            taskDistributor.execute(task);
        }
    }

    /**
     * Add to the write ahead log (hazelcast IMap) that tracks all the outstanding tasks
     */
    public boolean addPendingTask(HazeltaskTask<GROUP> task, boolean replaceIfExists) {
        if(!replaceIfExists)
            return pendingTask.putIfAbsent(task.getId(), task) == null;
        
        pendingTask.put(task.getId(), task);
        return true;
    }
    
    /**
     * Asynchronously put the work into the pending map so we can work on submitting it to the worker
     * if we wanted.  Could possibly cause duplicate work if we execute the work, then add to the map.
     * @param task
     * @return
     */
    public Future<HazeltaskTask<GROUP>> addPendingTaskAsync(HazeltaskTask<GROUP> task) {
        return pendingTask.putAsync(task.getId(), task);
    }

    public boolean removePendingTask(HazeltaskTask<GROUP> task) {
        pendingTask.removeAsync(task.getId());
        return true;
    }


    public boolean addToPreventDuplicateSetIfAbsent(String itemId) {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean removePreventDuplicateItem(String itemId) {
        // TODO Auto-generated method stub
        return false;
    }

    public void broadcastTaskCompletion(UUID taskId, Serializable response) {
        TaskResponse<Serializable> message = new TaskResponse<Serializable>(me, taskId, response, TaskResponse.Status.SUCCESS);
        taskResponseTopic.publish(message);
    }

    public void broadcastTaskCancellation(UUID taskId) {
        TaskResponse<Serializable> message = new TaskResponse<Serializable>(me, taskId, null, TaskResponse.Status.CANCELLED);
        taskResponseTopic.publish(message);
    }

    public void broadcastTaskError(UUID taskId, Throwable exception) {
        TaskResponse<Serializable> message = new TaskResponse<Serializable>(me, taskId, exception);
        taskResponseTopic.publish(message);
    }

    public Collection<HazeltaskTask<GROUP>> getLocalPendingTasks(String predicate) {
        Set<UUID> keys = pendingTask.localKeySet(new SqlPredicate(predicate));
        return pendingTask.getAll(keys).values();
    }

    public Collection<MemberResponse<Long>> getLocalQueueSizes() {
        return MemberTasks.executeOptimistic(
                communicationExecutorService, 
                topology.getReadyMembers(),
                new GetLocalQueueSizesOp<GROUP>(topology.getName())
        );
    }
    


    @Override
    public Collection<MemberResponse<Map<GROUP, Integer>>> getLocalGroupSizes() {
        return MemberTasks.executeOptimistic(
                communicationExecutorService, 
                topology.getReadyMembers(),
                new GetLocalGroupQueueSizesOp<GROUP>(topology.getName())
        );
    }

    public void addTaskResponseMessageHandler(MessageListener<TaskResponse<Serializable>> listener) {
        taskResponseTopic.addMessageListener(listener);
    }

    

    

    public Lock getRebalanceTaskClusterLock() {
        return rebalanceTasksLock;
    }

    @SuppressWarnings("unchecked")
    public Collection<HazeltaskTask<GROUP>> stealTasks(List<MemberValuePair<Long>> numToTake) {
        Collection<HazeltaskTask<GROUP>> result = new LinkedList<HazeltaskTask<GROUP>>();
        Collection<Future<Collection<HazeltaskTask<GROUP>>>> futures = new ArrayList<Future<Collection<HazeltaskTask<GROUP>>>>(numToTake.size());
        for(MemberValuePair<Long> entry : numToTake) {
            futures.add((Future<Collection<HazeltaskTask<GROUP>>>)
                    communicationExecutorService.submit(MemberTasks.create(new StealTasksOp<GROUP>(topology.getName(), entry.getValue()), entry.getMember())));
        }
        
        for(Future<Collection<HazeltaskTask<GROUP>>> f : futures) {
            try {
                Collection<HazeltaskTask<GROUP>> task = f.get(3, TimeUnit.MINUTES);//wait at most 3 minutes
                result.addAll(task);
            } catch (InterruptedException e) {
                LOGGER.log(Level.SEVERE,"Unable to take tasks. I was interrupted.  We may have pulled work out of another member... it will need to be recovered", e);
                Thread.currentThread().interrupt();
                return result;
            } catch (ExecutionException e) {
                LOGGER.log(Level.SEVERE,"Unable to take tasks. I got an exception.  We may have pulled work out of another member... it will need to be recovered", e);
                continue;
            } catch (TimeoutException e) {
                LOGGER.log(Level.SEVERE,"Unable to take tasks within 3 minutes.  We may have pulled work out of another member... it will need to be recovered");
                continue;
            } 
        }
        return result;
    }

    public int getLocalPendingTaskMapSize() {
        return pendingTask.localKeySet().size();
    }

    public Collection<MemberResponse<Long>> getOldestTaskTimestamps() {
        return MemberTasks.executeOptimistic(
             communicationExecutorService, 
             topology.getReadyMembers(),
             new GetOldestTimestampOp<GROUP>(topology.getName())
        );
    }
}
