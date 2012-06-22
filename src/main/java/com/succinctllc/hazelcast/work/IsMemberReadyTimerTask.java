package com.succinctllc.hazelcast.work;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.TimerTask;
import java.util.concurrent.Callable;

import com.hazelcast.core.Member;
import com.succinctllc.hazelcast.cluster.MemberTasks;
import com.succinctllc.hazelcast.cluster.MemberTasks.MemberResponse;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorService;

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
		
        Collection<MemberResponse<Boolean>> results = MemberTasks.executeOptimistic(
                t.getCommunicationExecutorService(), t.getHazelcast().getCluster().getMembers(),
                new IsReady(t.getName()));

        Member thisMember = t.getHazelcast().getCluster().getLocalMember();

        Collection<Member> members = new LinkedList<Member>();
        
        for (MemberResponse<Boolean> result : results) {
            if (result.getValue()) {
                Member m = result.getMember();
                // we need to make sure the member thinks its local if it is
                // hazelcast is dumb
                if (m.equals(thisMember)) m = thisMember;
                members.add(m);
            }
        }
        
        //set the ready members on the topology instance
        t.setReadyMembers(members);
	}
	
	public static class IsReady implements Callable<Boolean>, Serializable {
        private static final long serialVersionUID = 1L;
        private String            topology;

        public IsReady(String topology) {
            this.topology = topology;
        }

        public Boolean call() throws Exception {
            DistributedExecutorService svc = HazelcastWorkManager.getDistributedExecutorService(topology);
            return svc != null && svc.isReady();
        }
    }
}
