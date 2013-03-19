package com.hazeltask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazeltask.core.concurrent.BackoffTimer.BackoffTask;

/**
 * TODO: we can turn this into a heart beat task that monitors the state of the members
 *       perhaps take into account more things than just ready or not
 * 
 * @author Jason Clawson
 *
 */
public class IsMemberReadyTimerTask<ID extends Serializable, GROUP extends Serializable> extends BackoffTask implements MembershipListener {
	private final ITopologyService<ID, GROUP> topologyService;
	private final HazeltaskTopology<ID, GROUP> topology;
	
	public IsMemberReadyTimerTask(ITopologyService<ID, GROUP> topologyService, HazeltaskTopology<ID, GROUP> topology) {
		this.topologyService = topologyService;
		this.topology = topology;
	}
	
	@Override
	public boolean execute() {
	    Collection<Member> readyMembers = topologyService.getReadyMembers();
	    Member me = topology.getLocalMember();

        Collection<Member> members = new ArrayList<Member>(readyMembers.size());    
        for (Member m : readyMembers) {
           // we need to make sure the member thinks its local if it is
           // hazelcast is dumb
           if (m.equals(me)) m = me;
                
           if(!m.isLiteMember())
              members.add(m);
        }
        
        //set the ready members on the topology instance
        topology.setReadyMembers(members);
        
        return true;
	}

    public void memberAdded(MembershipEvent membershipEvent) {
        this.execute();
    }

    public void memberRemoved(MembershipEvent membershipEvent) {
        Member m = membershipEvent.getMember();
        Set<Member> members = topology.getReadyMembers();
        members.remove(m);
        topology.setReadyMembers(members);
    }
	
	
}
