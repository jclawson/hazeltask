package com.hazeltask.executor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MessageListener;
import com.hazelcast.logging.ILogger;
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

public class HazelcastExecutorTopologyService implements IExecutorTopologyService {
    //private final BloomFilter<CharSequence> bloomFilter;
    private HazeltaskTopology topology;
    private String topologyName;
    private final Member me;
    private ILogger LOGGER;
    
    
    private final ExecutorService communicationExecutorService;

    private final ExecutorService taskDistributor;
    //private final CopyOnWriteArrayListSet<Member> readyMembers;
    private final IMap<Serializable, HazeltaskTask>                            pendingTask;
    private final ILock rebalanceTasksLock;
    private final ITopic<TaskResponse>      taskResponseTopic;
    private final HazelcastInstance hazelcast;
    
    public HazelcastExecutorTopologyService(HazeltaskConfig hazeltaskConfig, HazeltaskTopology topology) {
        topologyName = hazeltaskConfig.getTopologyName();
        this.topology = topology;
        hazelcast = hazeltaskConfig.getHazelcast();
        this.me = hazelcast.getCluster().getLocalMember();
        this.LOGGER = topology.getLoggingService().getLogger(HazelcastExecutorTopologyService.class.getName());
        
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

    
    public boolean sendTask(HazeltaskTask task, Member member, boolean waitForAck) throws TimeoutException {
        @SuppressWarnings("unchecked")
        Future<Boolean> future = (Future<Boolean>) taskDistributor.submit(MemberTasks.create(new SubmitTaskOp(task, topologyName), member));
        if(waitForAck) {
            try {
                return future.get(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException e) {
                LOGGER.log(Level.SEVERE, "Unable to submit task for execution", e);
                return false;
            }
        } else {
            return true;
        }
    }

    /**
     * Add to the write ahead log (hazelcast IMap) that tracks all the outstanding tasks
     */
    public boolean addPendingTask(HazeltaskTask task, boolean replaceIfExists) {
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
    public Future<HazeltaskTask> addPendingTaskAsync(HazeltaskTask task) {
        return pendingTask.putAsync(task.getId(), task);
    }

    public boolean removePendingTask(HazeltaskTask task) {
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

    public void broadcastTaskCompletion(Serializable taskId, Serializable response) {
        TaskResponse message = new TaskResponse(me, taskId, response, TaskResponse.Status.SUCCESS);
        taskResponseTopic.publish(message);
    }

    public void broadcastTaskCancellation(Serializable taskId) {
        TaskResponse message = new TaskResponse(me, taskId, null, TaskResponse.Status.CANCELLED);
        taskResponseTopic.publish(message);
    }

    public void broadcastTaskError(Serializable taskId, Throwable exception) {
        TaskResponse message = new TaskResponse(me, taskId, exception);
        taskResponseTopic.publish(message);
    }

    public Collection<HazeltaskTask> getLocalPendingTasks(String predicate) {
        Set<Serializable> keys = pendingTask.localKeySet(new SqlPredicate(predicate));
        return pendingTask.getAll(keys).values();
    }

    public Collection<MemberResponse<Long>> getLocalQueueSizes() {
        return MemberTasks.executeOptimistic(
                communicationExecutorService, 
                topology.getReadyMembers(),
                new GetLocalQueueSizesOp(topology.getName())
        );
    }
    


    @Override
    public Collection<MemberResponse<Map<Serializable, Integer>>> getLocalGroupSizes() {
        return MemberTasks.executeOptimistic(
                communicationExecutorService, 
                topology.getReadyMembers(),
                new GetLocalGroupQueueSizesOp(topology.getName())
        );
    }

    public void addTaskResponseMessageHandler(MessageListener<TaskResponse> listener) {
        taskResponseTopic.addMessageListener(listener);
    }

    

    

    public Lock getRebalanceTaskClusterLock() {
        return rebalanceTasksLock;
    }

    @SuppressWarnings("unchecked")
    public Collection<HazeltaskTask> stealTasks(List<MemberValuePair<Long>> numToTake) {
        Collection<HazeltaskTask> result = new LinkedList<HazeltaskTask>();
        Collection<Future<Collection<HazeltaskTask>>> futures = new ArrayList<Future<Collection<HazeltaskTask>>>(numToTake.size());
        for(MemberValuePair<Long> entry : numToTake) {
            futures.add((Future<Collection<HazeltaskTask>>)
                    communicationExecutorService.submit(MemberTasks.create(new StealTasksOp(topology.getName(), entry.getValue()), entry.getMember())));
        }
        
        for(Future<Collection<HazeltaskTask>> f : futures) {
            try {
                Collection<HazeltaskTask> task = f.get(3, TimeUnit.MINUTES);//wait at most 3 minutes
                result.addAll(task);
            } catch (InterruptedException e) {
                //FIXME: log... we may have just dumped work into the ether.. it will have to be recovered
                //this really really should not happen
                Thread.currentThread().interrupt();
                return result;
            } catch (ExecutionException e) {
                //FIXME: log... we may have just dumped work into the ether.. it will have to be recovered
                continue;
            } catch (TimeoutException e) {
                //FIXME: log error... we just dumped work into the ether.. it will have to be recovered
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
             new GetOldestTimestampOp(topology.getName())
        );
    }

}
