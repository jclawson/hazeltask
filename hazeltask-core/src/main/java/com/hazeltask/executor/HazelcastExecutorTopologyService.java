package com.hazeltask.executor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Predicate;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MessageListener;
import com.hazelcast.query.SqlPredicate;
import com.hazeltask.HazeltaskTopology;
import com.hazeltask.clusterop.CancelTaskOp;
import com.hazeltask.clusterop.ClearGroupQueueOp;
import com.hazeltask.clusterop.GetLocalGroupQueueSizesOp;
import com.hazeltask.clusterop.GetLocalQueueSizesOp;
import com.hazeltask.clusterop.GetOldestTimestampOp;
import com.hazeltask.clusterop.GetThreadPoolSizesOp;
import com.hazeltask.clusterop.StealTasksOp;
import com.hazeltask.clusterop.SubmitTaskOp;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.executor.task.HazeltaskTask;
import com.hazeltask.executor.task.TaskResponse;
import com.hazeltask.hazelcast.MemberTasks;
import com.hazeltask.hazelcast.MemberTasks.MemberResponse;
import com.hazeltask.hazelcast.MemberValuePair;

@Slf4j
public class HazelcastExecutorTopologyService<GROUP extends Serializable> implements IExecutorTopologyService<GROUP> {
    //private final BloomFilter<CharSequence> bloomFilter;
    private HazeltaskTopology<GROUP> topology;
    private String topologyName;
    private final Member me;
    
    private final IExecutorService communicationExecutorService;

    private final IExecutorService taskDistributor;
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
                .setPoolSize(1)
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
    
    public void sendTask(HazeltaskTask<GROUP> task, Member member) throws TimeoutException {
    	SubmitTaskOp<GROUP> distTask = new SubmitTaskOp<GROUP>(task, topologyName);
        if(asyncTaskDistributorExecutor != null) {
            asyncTaskDistributorExecutor.execute(new $SendTaskToWorker(taskDistributor, member, distTask));
        } else {
            taskDistributor.submitToMember(distTask, member);
        }
    }
    
    @RequiredArgsConstructor
    private static class $SendTaskToWorker implements Runnable {
        private final IExecutorService taskDistributor;
        private final Member member;
        private final Callable<Boolean> task;
        
        @Override
        public void run() {
            taskDistributor.submitToMember(task, member);
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
        return removePendingTask(task.getId());
    }
    
    public boolean removePendingTask(UUID taskId) {
    	pendingTask.removeAsync(taskId);
    	return true;
    }

    public void broadcastTaskCompletion(UUID taskId, Serializable response, Serializable taskInfo) {
        TaskResponse<Serializable> message = new TaskResponse<Serializable>(me, taskId, taskInfo, response, TaskResponse.Status.SUCCESS);
        taskResponseTopic.publish(message);
    }

    public void broadcastTaskCancellation(UUID taskId, Serializable taskInfo) {
        TaskResponse<Serializable> message = new TaskResponse<Serializable>(me, taskId, taskInfo, null, TaskResponse.Status.CANCELLED);
        taskResponseTopic.publish(message);
    }

    public void broadcastTaskError(UUID taskId, Throwable exception, Serializable taskInfo) {
        TaskResponse<Serializable> message = new TaskResponse<Serializable>(me, taskId, taskInfo, exception);
        taskResponseTopic.publish(message);
    }

    public Collection<HazeltaskTask<GROUP>> getLocalPendingTasks(String predicate) {
        Set<UUID> keys = pendingTask.localKeySet(new SqlPredicate(predicate));
        return pendingTask.getAll(keys).values();
    }

    public Collection<MemberResponse<Long>> getMemberQueueSizes() {
        return MemberTasks.executeOptimistic(
                communicationExecutorService, 
                topology.getReadyMembers(),
                new GetLocalQueueSizesOp<GROUP>(topology.getName())
        );
    }
    


    @Override
    public Collection<MemberResponse<Map<GROUP, Integer>>> getMemberGroupSizes() {
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

    public Collection<HazeltaskTask<GROUP>> stealTasks(List<MemberValuePair<Long>> numToTake) {
        Collection<HazeltaskTask<GROUP>> result = new LinkedList<HazeltaskTask<GROUP>>();
        Collection<Future<Collection<HazeltaskTask<GROUP>>>> futures = new ArrayList<Future<Collection<HazeltaskTask<GROUP>>>>(numToTake.size());
        for(MemberValuePair<Long> entry : numToTake) {    	
        	futures.add(communicationExecutorService
        					.submitToMember(
        							new StealTasksOp<GROUP>(topology.getName(), entry.getValue()), 
        							entry.getMember()
        					)
        				);
        }
        
        for(Future<Collection<HazeltaskTask<GROUP>>> f : futures) {
            try {
                Collection<HazeltaskTask<GROUP>> task = f.get(3, TimeUnit.MINUTES);//wait at most 3 minutes
                result.addAll(task);
            } catch (InterruptedException e) {
                log.error("Unable to take tasks. I was interrupted.  We may have pulled work out of another member... it will need to be recovered", e);
                Thread.currentThread().interrupt();
                return result;
            } catch (ExecutionException e) {
                log.error("Unable to take tasks. I got an exception.  We may have pulled work out of another member... it will need to be recovered", e);
                continue;
            } catch (TimeoutException e) {
                log.error("Unable to take tasks within 3 minutes.  We may have pulled work out of another member... it will need to be recovered");
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
    
    @Override
    public Collection<MemberResponse<Integer>> getThreadPoolSizes() {
        return MemberTasks.executeOptimistic(
                communicationExecutorService, 
                topology.getReadyMembers(),
                new GetThreadPoolSizesOp<GROUP>(topology.getName())
        );
    }

    @Override
    public Collection<MemberResponse<Map<GROUP, Integer>>> getGroupSizes(Predicate<GROUP> predicate) {
        if(predicate != null && !(predicate instanceof Serializable)) {
            //using illegalargument instead of notserializable because its dumb to have a checked exception here
            throw new IllegalArgumentException(predicate.getClass().getName()+" is not serializable");
        }
        
        return MemberTasks.executeOptimistic(
                communicationExecutorService, 
                topology.getReadyMembers(),
                new GetLocalGroupQueueSizesOp<GROUP>(topology.getName(), predicate)
        );
    }

    @Override
    public void clearGroupQueue(GROUP group) {
        MemberTasks.executeOptimistic(
                communicationExecutorService, 
                topology.getReadyMembers(),
                new ClearGroupQueueOp<GROUP>(topology.getName(), group)
        );
    }

    @Override
    public boolean cancelTask(GROUP group, UUID taskId) {
        Collection<MemberResponse<Boolean>> responses = MemberTasks.executeOptimistic(
             communicationExecutorService, 
             topology.getReadyMembers(),
             new CancelTaskOp<GROUP>(topology.getName(), taskId, group)
        );
        
        for(MemberResponse<Boolean> response : responses) {
            if(response.getValue() == true)
                return true;
        }
        return false;
    }
}
