package com.hazeltask.config;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.io.Serializable;

import com.hazelcast.core.Member;
import com.hazeltask.core.concurrent.collections.grouped.prioritizer.EnumOrdinalPrioritizer;
import com.hazeltask.core.concurrent.collections.grouped.prioritizer.GroupPrioritizer;
import com.hazeltask.core.concurrent.collections.grouped.prioritizer.LoadBalancedPriorityPrioritizer;
import com.hazeltask.core.concurrent.collections.grouped.prioritizer.RoundRobinGroupPrioritizer;
import com.hazeltask.core.concurrent.collections.router.ListRouterFactory;
import com.hazeltask.core.concurrent.collections.router.RoundRobinRouter;

public class ExecutorLoadBalancingConfig<GROUP extends Serializable> {
    private ListRouterFactory<Member>  memberRouterFactory      = RoundRobinRouter.newFactory();
    //private ListRouterFactory<Entry<GROUP, ITrackedQueue<HazeltaskTask<GROUP>>>>  taskRouterFactory        = RoundRobinRouter.newFactory();
    private GroupPrioritizer<GROUP> groupPrioritizer = new RoundRobinGroupPrioritizer<GROUP>();
    private long               rebalanceTaskPeriod      = MINUTES.toMillis(2);
    
    public static <GROUP extends Serializable> ExecutorLoadBalancingConfig<GROUP> create() {
        return new ExecutorLoadBalancingConfig<GROUP>();
    }
    
    public ExecutorLoadBalancingConfig<GROUP> useRoundRobinPrioritizer() {
        groupPrioritizer = new RoundRobinGroupPrioritizer<GROUP>();
        return this;
    }
    
    public ExecutorLoadBalancingConfig<GROUP> useEnumOrdinalPrioritizer(Class<GROUP> groupClass) {
        if(!groupClass.isEnum()) {
            throw new IllegalArgumentException("The group class "+groupClass+" is not an enum");
        }
        groupPrioritizer = new EnumOrdinalPrioritizer<GROUP>();
        return this;
    }
    
    public ExecutorLoadBalancingConfig<GROUP> useLoadBalancedEnumOrdinalPrioritizer(Class<GROUP> groupClass) {
        if(!groupClass.isEnum()) {
            throw new IllegalArgumentException("The group class "+groupClass+" is not an enum");
        }
        groupPrioritizer = new LoadBalancedPriorityPrioritizer<GROUP>(new EnumOrdinalPrioritizer<GROUP>());
        return this;
    }
    
    public ExecutorLoadBalancingConfig<GROUP> useCustomPrioritizer(GroupPrioritizer<GROUP> prioritizer) {
        groupPrioritizer = prioritizer;
        return this;
    }
    
    public GroupPrioritizer<GROUP> getGroupPrioritizer() {
        return groupPrioritizer;
    }

    public ListRouterFactory<Member> getMemberRouterFactory() {
        return this.memberRouterFactory;
    }
    
    public ExecutorLoadBalancingConfig<GROUP> withRebalanceTaskPeriod(long rebalanceTaskPeriod) {
        this.rebalanceTaskPeriod = rebalanceTaskPeriod;
        return this;
    }
   
    public long getRebalanceTaskPeriod() {
        return this.rebalanceTaskPeriod;
    }
    
}
