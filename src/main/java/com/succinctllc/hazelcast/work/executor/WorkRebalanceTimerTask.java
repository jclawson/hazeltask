package com.succinctllc.hazelcast.work.executor;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.TimerTask;

import com.hazelcast.core.Member;
import com.succinctllc.hazelcast.util.MemberTasks.MemberResponse;

/**
 * TODO: for this, lets lock the cluster down so we can figure out exactly 
 * what members, should take how much work exactly and then tell them about it
 * 
 * @author jclawson
 */
public class WorkRebalanceTimerTask extends TimerTask {
	private final DistributedExecutorService distributedExecutorService;
	
	/**
	 * If a member has this percent MORE works than the current member, steal them
	 */
	private static final double PERCENT_THRESHOLD = 0.30;
	
	public WorkRebalanceTimerTask(DistributedExecutorService distributedExecutorService) {
		this.distributedExecutorService = distributedExecutorService;
	}
	
	private static class MemberValuePair {
		Member member;
		long value;
		public MemberValuePair(Member member, long value) {
			this.member = member;
			this.value = value;
		}
	}
	
	@Override
	public void run() {
		ClusterServices clusterServices = distributedExecutorService.getTopology().getClusterServices();
		Collection<MemberResponse<Long>> queueSizes = clusterServices.getLocalQueueSizes();
		long totalSize = 0;
		for(MemberResponse<Long> response : queueSizes) {
			totalSize += response.getValue();
		}
		
		long optimalSize = totalSize / queueSizes.size();
		
		
		Member member = this.distributedExecutorService.getTopology().getHazelcast().getCluster().getLocalMember();
		long localQueueSize = distributedExecutorService.getLocalExecutorService().getQueueSize();
		List<MemberValuePair> adjustements = new LinkedList<MemberValuePair>();
		long totalToSteal = 0;
		long returnAdjustmentNotMeetingThreshold = 0;
		
		for(MemberResponse<Long> response : queueSizes) {
//			if(!member.equals(response.getMember())) {
//				long numToSteal = Math.round(response.getValue() * PERCENT_THRESHOLD);
//				if((response.getValue() - numToSteal) >= localQueueSize) {				
//					steals.add(new MemberStealCount(response.getMember(), numToSteal));
//					totalToSteal += numToSteal;
//				}
//			}
			
			long adjustment = optimalSize - response.getValue();
			if(Math.abs(adjustment) < response.getValue()*PERCENT_THRESHOLD) {
				returnAdjustmentNotMeetingThreshold += adjustment;
			} else {
				adjustements.add(new MemberValuePair(member, adjustment));
			}
			
		}
		
//		long returnedLeft = returnedItems;
//		for(int i =0; i<adjustments.size(); i++) {
//		    if(adjustments[i] != 0) {
//		       //FIXME: we might have a remainder!
//		       adjustments[i]+= returnEach;
//		       returnedLeft-= returnEach;
//		       println("left "+returnedLeft+" -- "+returnedItems);
//		       if((returnedItems < 0 && (returnedLeft - returnEach) >= 0) || (returnedItems > 0 && (returnedLeft - returnEach) <=0) ) {
//		           adjustments[i]+= returnedLeft;
//		       }
//		    }
//		}
		
		//from everyone I am stealing from... I should not be PERCENT_THRESHOLD more than them
	}
}
