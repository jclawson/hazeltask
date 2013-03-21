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
    private ListRouterFactory<Member> memberRouterFactory = RoundRobinRouter.newFactory();
    private GroupPrioritizer<GROUP>   groupPrioritizer    = new RoundRobinGroupPrioritizer<GROUP>();
    private long                      rebalanceTaskPeriod = MINUTES.toMillis(2);

    public static <GROUP extends Serializable> ExecutorLoadBalancingConfig<GROUP> create() {
        return new ExecutorLoadBalancingConfig<GROUP>();
    }
    
    public ExecutorLoadBalancingConfig<GROUP> useRoundRobinPrioritizer() {
        groupPrioritizer = new RoundRobinGroupPrioritizer<GROUP>();
        return this;
    }
    
    /**
     * Use this if you have an enum used for your group type that defines the priority of a task and
     * you ALWAYS want to execute high priority items over low priority items.  You should really 
     * check out the <code>useLoadBalancedEnumOrdinalPrioritizer</code> which works similarly but 
     * prevents starvation of low priority items by allowing them to run some of the time
     * 
     * @param groupClass
     * @return
     */
    public ExecutorLoadBalancingConfig<GROUP> useEnumOrdinalPrioritizer(Class<GROUP> groupClass) {
        if(!groupClass.isEnum()) {
            throw new IllegalArgumentException("The group class "+groupClass+" is not an enum");
        }
        groupPrioritizer = new EnumOrdinalPrioritizer<GROUP>();
        return this;
    }
    
    /**
     * If you have priorities based on enums, this is the recommended prioritizer to use as it will prevent
     * starvation of low priority items
     * 
     * @param groupClass
     * @return
     */
    public ExecutorLoadBalancingConfig<GROUP> useLoadBalancedEnumOrdinalPrioritizer(Class<GROUP> groupClass) {
        if(!groupClass.isEnum()) {
            throw new IllegalArgumentException("The group class "+groupClass+" is not an enum");
        }
        groupPrioritizer = new LoadBalancedPriorityPrioritizer<GROUP>(new EnumOrdinalPrioritizer<GROUP>());
        return this;
    }
    
    /**
     * Use this config option if you would like to create your own custom task group prioritizer
     * 
     * @param prioritizer
     * @return
     */
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
    
    /**
     * This is the period in which we will rebalance the task load across nodes ensuring all nodes
     * have roughly the same number of tasks.  By default this is set to 2 minutes.
     * 
     * @param rebalanceTaskPeriod
     * @return
     */
    public ExecutorLoadBalancingConfig<GROUP> withRebalanceTaskPeriod(long rebalanceTaskPeriod) {
        this.rebalanceTaskPeriod = rebalanceTaskPeriod;
        return this;
    }
   
    public long getRebalanceTaskPeriod() {
        return this.rebalanceTaskPeriod;
    }
    
}
