package com.hazeltask.executor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
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
import com.hazeltask.clustertasks.GetLocalQueueSizesTask;
import com.hazeltask.clustertasks.StealTasksTask;
import com.hazeltask.clustertasks.SubmitTaskTask;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.executor.task.HazelcastWork;
import com.hazeltask.executor.task.WorkResponse;
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

    private final ExecutorService workDistributor;
    //private final CopyOnWriteArrayListSet<Member> readyMembers;
    private final IMap<String, HazelcastWork>                            pendingWork;
    private final ILock rebalanceTasksLock;
    private final ITopic<WorkResponse>      workResponseTopic;
    private final HazelcastInstance hazelcast;
    
    public HazelcastExecutorTopologyService(HazeltaskConfig hazeltaskConfig, HazeltaskTopology topology) {
        topologyName = hazeltaskConfig.getTopologyName();
        this.topology = topology;
        hazelcast = hazeltaskConfig.getHazelcast();
        this.me = hazelcast.getCluster().getLocalMember();
        this.LOGGER = topology.getLoggingService().getLogger(HazelcastExecutorTopologyService.class.getName());
        
        communicationExecutorService = hazelcast.getExecutorService(name("com"));
        
        String workDistributorName = name("work-distributor");
        
        //limit the threads on the distributor to 1 thread
        hazelcast.getConfig()
            .addExecutorConfig(new ExecutorConfig()
                .setName(workDistributorName)
                .setMaxPoolSize(1)
                .setCorePoolSize(1)
            );
        
        workDistributor =  hazelcast.getExecutorService(workDistributorName);
        //readyMembers = new CopyOnWriteArrayListSet<Member>();
        
        String pendingWorkMapName = name("pending-work");
        hazelcast.getConfig()
        .addMapConfig(new MapConfig()
            .setName(workDistributorName)
            .addMapIndexConfig(new MapIndexConfig("createdAtMillis", false)));
        
        pendingWork = hazelcast.getMap(pendingWorkMapName);
        workResponseTopic = hazelcast.getTopic(name("work-response"));
        
        rebalanceTasksLock = hazelcast.getLock(name("task-balance"));
    }
    
    private String name(String name) {
        return topologyName + "-" + name;
    }
    
//    public boolean isMemberReady(Member member) {
//        // TODO Auto-generated method stub
//        return false;
//    }

    
    public boolean sendTask(HazelcastWork work, Member member, boolean waitForAck) throws TimeoutException {
        @SuppressWarnings("unchecked")
        Future<Boolean> future = (Future<Boolean>) workDistributor.submit(MemberTasks.create(new SubmitTaskTask(work, topologyName), member));
        if(!waitForAck) {
            try {
                return future.get(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException e) {
                LOGGER.log(Level.SEVERE, "Unable to submit work for execution", e);
                return false;
            }
        } else {
            return true;
        }
    }

    /**
     * Add to the write ahead log (hazelcast IMap) that tracks all the outstanding tasks
     */
    public boolean addPendingTask(HazelcastWork work, boolean replaceIfExists) {
        if(!replaceIfExists)
            return pendingWork.putIfAbsent(work.getUniqueIdentifier(), work) == null;
        
        pendingWork.put(work.getUniqueIdentifier(), work);
        return true;
    }
    
    /**
     * Asynchronously put the work into the pending map so we can work on submitting it to the worker
     * if we wanted.  Could possibly cause duplicate work if we execute the work, then add to the map.
     * @param work
     * @return
     */
    public Future<HazelcastWork> addPendingTaskAsync(HazelcastWork work) {
        return pendingWork.putAsync(work.getUniqueIdentifier(), work);
    }

    public boolean removePendingTask(HazelcastWork work) {
        pendingWork.removeAsync(work.getUniqueIdentifier());
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

    public void broadcastTaskCompletion(String workId, Serializable response) {
        WorkResponse message = new WorkResponse(me, workId, response, WorkResponse.Status.SUCCESS);
        workResponseTopic.publish(message);
    }

    public void broadcastTaskCancellation(String workId) {
        WorkResponse message = new WorkResponse(me, workId, null, WorkResponse.Status.CANCELLED);
        workResponseTopic.publish(message);
    }

    public void broadcastTaskError(String workId, Throwable exception) {
        WorkResponse message = new WorkResponse(me, workId, exception);
        workResponseTopic.publish(message);
    }

    public Collection<HazelcastWork> getLocalPendingTasks(String predicate) {
        Set<String> keys = pendingWork.localKeySet(new SqlPredicate(predicate));
        return pendingWork.getAll(keys).values();
    }

    public Collection<MemberResponse<Long>> getLocalQueueSizes() {
        return MemberTasks.executeOptimistic(
                communicationExecutorService, 
                topology.getReadyMembers(),
                new GetLocalQueueSizesTask(topology.getName())
        );
    }

    public void addTaskResponseMessageHandler(MessageListener<WorkResponse> listener) {
        workResponseTopic.addMessageListener(listener);
    }

    

    

    public Lock getRebalanceTaskClusterLock() {
        return rebalanceTasksLock;
    }

    @SuppressWarnings("unchecked")
    public Collection<HazelcastWork> stealTasks(List<MemberValuePair<Long>> numToTake) {
        Collection<HazelcastWork> result = new LinkedList<HazelcastWork>();
        Collection<Future<Collection<HazelcastWork>>> futures = new ArrayList<Future<Collection<HazelcastWork>>>(numToTake.size());
        for(MemberValuePair<Long> entry : numToTake) {
            futures.add((Future<Collection<HazelcastWork>>)
                    communicationExecutorService.submit(MemberTasks.create(new StealTasksTask(topology.getName(), entry.getValue()), entry.getMember())));
        }
        
        for(Future<Collection<HazelcastWork>> f : futures) {
            try {
                Collection<HazelcastWork> work = f.get(3, TimeUnit.MINUTES);//wait at most 3 minutes
                result.addAll(work);
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

    public int getLocalPendingWorkMapSize() {
        return pendingWork.localKeySet().size();
    }

}
