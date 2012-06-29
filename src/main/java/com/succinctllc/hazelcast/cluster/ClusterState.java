package com.succinctllc.hazelcast.cluster;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.Partition;

/**
 * This is not currently used but its a neat idea to keep track of
 * what partitions are migrating to what members since hazelcast doesn't
 * really tell us all the needed info... for example when a node goes down
 * and a backup comes online... there is no migration event
 *
 */
public class ClusterState implements MigrationListener, MembershipListener {
	private HazelcastInstance hazelcast;
	
	private Multimap<Member, Integer> memberToPartition = HashMultimap.create();
	private Map<Integer, Member>	  partitionToMember = new HashMap<Integer, Member>();
	private Collection<MigrationListener> migrationListeners = new LinkedList<MigrationListener>();
	
	private ReentrantLock lock = new ReentrantLock();
	
	public ClusterState(HazelcastInstance hazelcast){
		this.hazelcast = hazelcast;
		
		hazelcast.getPartitionService().addMigrationListener(this);
		hazelcast.getCluster().addMembershipListener(this);
		
		//initialize partition owners -----------------------------------------------
		this.syncPartitions();
	}
	
	private void syncPartitions(){
		lock.lock();
		try {
			Set<Partition> partitions = hazelcast.getPartitionService().getPartitions();
			for(Partition p : partitions) {
				Member m = partitionToMember.get(p.getPartitionId());
				changePartitionOwner(p.getPartitionId(), m, p.getOwner(), true);
			}
		} finally {
			lock.unlock();
		}
	}
	
	private void changePartitionOwner(int partitionId, Member from, Member to, boolean sendStartedEvent) {
		lock.lock();
		try {
			memberToPartition.put(to, partitionId);
			partitionToMember.put(partitionId, to);
			if(from != null) {
				MigrationEvent event = new MigrationEvent(this, partitionId, from, to);
				for(MigrationListener listener : migrationListeners) {
					if(sendStartedEvent)
						listener.migrationStarted(event);
					listener.migrationCompleted(event);
				}
			}
		} finally {
			lock.unlock();
		}
	}
	
	public void addListener(MigrationListener listener){
		migrationListeners.add(listener);
	}
	
//	public Member getPartitionOwner(int partitionId) {
//		PartitionManager pMan = getPartitionManager();
//		Address addr = pMan.getOwner(partitionId);
//		return pMan.getMember(addr);
//	}
//	
//	public ConcurrentMapManager getConcurrentMapManager() {
//        FactoryImpl factory = (FactoryImpl) hazelcast;
//        return factory.node.concurrentMapManager;
//    }
//	
//	public PartitionManager getPartitionManager(){
//		return getConcurrentMapManager().getPartitionManager();
//	}
	
	

	public void memberAdded(MembershipEvent membershipEvent) {
		// TODO Auto-generated method stub
	}

	public void memberRemoved(MembershipEvent membershipEvent) {
		syncPartitions();
	}

	public void migrationStarted(MigrationEvent migrationEvent) {
		for(MigrationListener listener : migrationListeners) {
			listener.migrationStarted(migrationEvent);
		}
	}

	public void migrationCompleted(MigrationEvent migrationEvent) {
		changePartitionOwner(migrationEvent.getPartitionId(), migrationEvent.getOldOwner(), migrationEvent.getNewOwner(), false);
	}
}
