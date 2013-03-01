package com.hazeltask.config;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import com.hazelcast.core.Member;
import com.hazeltask.config.helpers.AbstractTaskRouterFactory;
import com.hazeltask.core.concurrent.collections.router.ListRouter;
import com.hazeltask.core.concurrent.collections.router.ListRouterFactory;
import com.hazeltask.core.concurrent.collections.router.LoadBalancedRouter;
import com.hazeltask.core.concurrent.collections.router.RoundRobinRouter;
import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;
import com.hazeltask.executor.task.HazeltaskTask;

public class ExecutorLoadBalancingConfig<ID extends Serializable, GROUP extends Serializable> {
    private ListRouterFactory<Member>  memberRouterFactory      = RoundRobinRouter.newFactory();
    private ListRouterFactory<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID,GROUP>>>>  taskRouterFactory        = RoundRobinRouter.newFactory();
    private long               rebalanceTaskPeriod      = MINUTES.toMillis(2);
    
    public static <ID extends Serializable, GROUP extends Serializable> ExecutorLoadBalancingConfig<ID,GROUP> create() {
        return new ExecutorLoadBalancingConfig<ID,GROUP>();
    }
    
    public ExecutorLoadBalancingConfig<ID,GROUP> useRoundRobinTaskGroupSelection() {
        taskRouterFactory = RoundRobinRouter.newFactory();
        return this;
    }
    
    public ExecutorLoadBalancingConfig<ID,GROUP> useComparatorTaskGroupSelection(final Comparator<GROUP> comparator) {
        final Comparator<Entry<GROUP, ITrackedQueue<?>>> comparatorWrapper = new Comparator<Entry<GROUP, ITrackedQueue<?>>>() {
            public int compare(Entry<GROUP, ITrackedQueue<?>> o1, Entry<GROUP, ITrackedQueue<?>> o2) {
                return comparator.compare(o1.getKey(), o2.getKey());
            }
        };
        
        AbstractTaskRouterFactory<ID, GROUP> myRouterFactory = new AbstractTaskRouterFactory<ID, GROUP>(){
            @Override
            public ListRouter<Entry<GROUP, ITrackedQueue<?>>> createTaskRouter(
                    Callable<List<Entry<GROUP, ITrackedQueue<?>>>> listAcessor) {
                return new LoadBalancedRouter<Map.Entry<GROUP,ITrackedQueue<?>>>(listAcessor, comparatorWrapper);
            } 
        };
        
        this.taskRouterFactory = myRouterFactory;
        return this;
    }
    
    /**
     * @deprecated - the signature of this method will very likely change
     * @param router
     * @return
     */
    @Deprecated
    public ExecutorLoadBalancingConfig<ID,GROUP> useCustomTaskGroupSelection(AbstractTaskRouterFactory<ID, GROUP> router) {
        this.taskRouterFactory = (ListRouterFactory<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID,GROUP>>>>) router;
        return this;
    }
    
    public ListRouterFactory<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID,GROUP>>>> getTaskRouterFactory() {
        return (ListRouterFactory<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID,GROUP>>>>) this.taskRouterFactory;
    }
    
    public ListRouterFactory<Member> getMemberRouterFactory() {
        return this.memberRouterFactory;
    }
    
    public ExecutorLoadBalancingConfig<ID, GROUP> withRebalanceTaskPeriod(long rebalanceTaskPeriod) {
        this.rebalanceTaskPeriod = rebalanceTaskPeriod;
        return this;
    }
   
    public long getRebalanceTaskPeriod() {
        return this.rebalanceTaskPeriod;
    }
    
}
