package com.succinctllc.hazelcast.work;

import java.util.Collection;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.succinctllc.core.concurrent.collections.CopyOnWriteArrayListSet;
import com.succinctllc.hazelcast.work.executor.ClusterServices;

/**
 * The topology of the works system describes the different services and
 * properties that are used to connect, and distribute work throughout the
 * cluster. It keeps track of - topology name - the hazelcast instance -
 * communication executor service - local topology index (for naming threads)
 * 
 * @author Jason Clawson
 * 
 */
public class HazelcastWorkTopology {
	public static AtomicInteger atomicIndex = new AtomicInteger(1);
	public static ConcurrentMap<String, HazelcastWorkTopology> instances = new ConcurrentHashMap<String, HazelcastWorkTopology>();

	public static long READY_MEMBER_PING_PERIOD = 30000; //every 30 seconds
	
	public static HazelcastWorkTopology getOrCreate(String topologyName, HazelcastInstance hazelcast) {
		HazelcastWorkTopology t = instances.get(topologyName);
		if (t == null) {
			t = new HazelcastWorkTopology(topologyName, hazelcast);
			if (instances.putIfAbsent(topologyName, t) != null) {
				t = instances.get(topologyName);
			} else {
				//start looking for members indicating they are ready to recieve work
				t.startReadyMemberPing();
			}
		}
		return t;
	}
	
	public static HazelcastWorkTopology getDefault(HazelcastInstance hazelcast){
		return getOrCreate("default", hazelcast);
	}
	
	public static HazelcastWorkTopology get(String name) {
		return instances.get(name);
	}

	private final String name;
	private final HazelcastInstance hazelcast;
	private final ExecutorService communicationExecutorService;
	private final int localTopologyId;
	private final ExecutorService workDistributor;
	private final CopyOnWriteArrayListSet<Member> readyMembers;
	private final IMap<String, HazelcastWork>                               pendingWork;
	private final ClusterServices clusterServices;
	
	
	/**
	 * This topic alerts all nodes to work completions.  Some nodes may have Futures
	 * waiting on results.
	 */
	private final ITopic<WorkResponse>      workResponseTopic;

	private HazelcastWorkTopology(String topologyName, HazelcastInstance hazelcast) {
		this.name = topologyName;
		this.hazelcast = hazelcast;
		this.localTopologyId = atomicIndex.getAndIncrement();
		communicationExecutorService = hazelcast.getExecutorService(createName("com"));
		
		String workDistributorName = createName("work-distributor");
		
		//limit the threads on the distributor to 1 thread
		hazelcast.getConfig()
		    .addExecutorConfig(new ExecutorConfig()
		        .setName(workDistributorName)
		        .setMaxPoolSize(1)
		        .setCorePoolSize(1)
		    );
		
		workDistributor =  hazelcast.getExecutorService(workDistributorName);
		readyMembers = new CopyOnWriteArrayListSet<Member>();
		
		String pendingWorkMapName = createName("pending-work");
		//add the createdAtMillis index to our pending-work map
		hazelcast.getConfig()
		    .addMapConfig(new MapConfig()
		    	.setName(workDistributorName)
		        .addMapIndexConfig(new MapIndexConfig("createdAtMillis", false)));
		
		pendingWork = hazelcast.getMap(pendingWorkMapName);
		//workFutures = hazelcast.getMultiMap(createName("work-futures"));
		workResponseTopic = hazelcast.getTopic(createName("work-response"));
		
		hazelcast.getConfig().setManagedContext(HazelcastWorkManagedContext.wrap(hazelcast.getConfig().getManagedContext()));
		
		clusterServices = new ClusterServices(this);
	}
	
	private void startReadyMemberPing() {
		Timer timer = new Timer(createName("ready-member-ping"), true);
        timer.schedule(new IsMemberReadyTimerTask(this), 0, READY_MEMBER_PING_PERIOD);
        
        //this listener will keep our ready members up to date with who is online
        this.hazelcast.getCluster().addMembershipListener(new MemberRemovedListener());
	}
	
	public ClusterServices getClusterServices() {
		return clusterServices;
	}
	
	/**
	 * If we start the local executor service, then lets let the local topology cache know
	 * immediately so we don't have to wait for the READY_MEMBER_PING_PERIOD
	 */
	public void localExecutorServiceReady() {
	    this.readyMembers.add(this.hazelcast.getCluster().getLocalMember());
	}
	
	public ITopic<WorkResponse> getWorkResponseTopic() {
	    return this.workResponseTopic;
	}
	
	public String createName(String name) {
        return this.name + "-" + name;
    }

	public IMap<String, HazelcastWork> getPendingWork() {
	    return pendingWork;
	}
	
	public String getName() {
		return name;
	}

	public HazelcastInstance getHazelcast() {
		return hazelcast;
	}

	public ExecutorService getCommunicationExecutorService() {
		return communicationExecutorService;
	}

	public int getLocalTopologyId() {
		return localTopologyId;
	}
	
	public ExecutorService getWorkDistributor() {
		return workDistributor;
	}

	public CopyOnWriteArrayListSet<Member> getReadyMembers() {
		return readyMembers;
	}

	public String toString() {
		return "Topology["+localTopologyId+"]: "+name;
	}
	
	/**
	 * Used by the IsMemberReadyTask to set the hazelcast cluster members that are 
	 * ready to do work
	 * 
	 * @param members
	 */
	protected void setReadyMembers(Collection<Member> members) {
		readyMembers.addAll(members);
	}
	
	private class MemberRemovedListener implements MembershipListener {
        public void memberAdded(MembershipEvent membershipEvent) {}

        public void memberRemoved(MembershipEvent membershipEvent) {
            readyMembers.remove(membershipEvent.getMember());
        }
	    
	}
}
