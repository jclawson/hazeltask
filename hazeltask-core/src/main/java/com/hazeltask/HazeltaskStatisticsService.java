package com.hazeltask;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import com.google.common.base.Predicate;
import com.hazeltask.executor.IExecutorTopologyService;
import com.hazeltask.hazelcast.MemberTasks.MemberResponse;

public class HazeltaskStatisticsService<GROUP extends Serializable> implements ClusterService<GROUP> {
    private final IExecutorTopologyService<GROUP> executorTopologyService;
    
    public HazeltaskStatisticsService(IExecutorTopologyService<GROUP> executorTopologyService) {
        this.executorTopologyService = executorTopologyService;
    }

    @Override
    public Collection<MemberResponse<Long>> getQueueSizes() {
        return executorTopologyService.getMemberQueueSizes();
    }

    @Override
    public Collection<MemberResponse<Map<GROUP, Integer>>> getGroupSizes() {
        return executorTopologyService.getMemberGroupSizes();
    }

    @Override
    public Collection<MemberResponse<Long>> getOldestTaskTimes() {
        return executorTopologyService.getOldestTaskTimestamps();
    }

    @Override
    public Collection<MemberResponse<Integer>> getThreadPoolSizes() {
        return executorTopologyService.getThreadPoolSizes();
    }

    @Override
    public Collection<MemberResponse<Map<GROUP, Integer>>> getGroupSizes(Predicate<GROUP> predicate) {
        return executorTopologyService.getGroupSizes(predicate);
    }

    @Override
    public void clearGroupQueue(GROUP group) {
        executorTopologyService.clearGroupQueue(group);
    }

}
