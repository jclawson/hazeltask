package com.succinctllc.hazelcast.work;

import java.util.ArrayList;
import java.util.Collection;
import java.util.TimerTask;

import com.hazelcast.core.Member;

/**
 * TODO: we can turn this into a heart beat task that monitors the state of the members
 * 
 * @author Jason Clawson
 *
 */
public class IsMemberReadyTimerTask extends TimerTask {
	private final HazelcastWorkTopology t;
	
	public IsMemberReadyTimerTask(HazelcastWorkTopology t) {
		this.t = t;
	}
	
	@Override
	public void run() {
	    Collection<Member> readyMembers = t.getClusterServices().getReadyMembers();
        Member thisMember = t.getHazelcast().getCluster().getLocalMember();

        Collection<Member> members = new ArrayList<Member>(readyMembers.size());    
        for (Member m : readyMembers) {
           // we need to make sure the member thinks its local if it is
           // hazelcast is dumb
           if (m.equals(thisMember)) m = thisMember;
                
           if(!m.isLiteMember())
              members.add(m);
        }
        
        //set the ready members on the topology instance
        t.setReadyMembers(members);
	}
	
	
}
