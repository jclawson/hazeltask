package com.hazeltask.batch;

import java.util.concurrent.CopyOnWriteArrayList;

import com.hazeltask.HazeltaskService;
import com.hazeltask.HazeltaskServiceListener;
import com.hazeltask.HazeltaskTopology;
import com.hazeltask.config.BundlerConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.concurrent.collections.grouped.Groupable;
import com.hazeltask.executor.DistributedExecutorService;

//TODO: can we create StatsTaskBatchingService that attaches stats tracking?
//use listeners too

public class TaskBatchingService<I extends Groupable> implements HazeltaskService<TaskBatchingService<I>> {
    
    private final HazeltaskConfig hazeltaskConfig;
    private final BundlerConfig<I> batchingConfig;
    private final DistributedExecutorService svc;
    private final HazeltaskTopology topology;
    private final IBatchClusterService<I> batchClusterService;
    private CopyOnWriteArrayList<HazeltaskServiceListener<TaskBatchingService<I>>> listeners = new CopyOnWriteArrayList<HazeltaskServiceListener<TaskBatchingService<I>>>();
    private final CopyOnWriteArrayList<BatchExecutorListener<I>> batchListeners = new CopyOnWriteArrayList<BatchExecutorListener<I>>();
    
    public TaskBatchingService(HazeltaskConfig hazeltaskConfig, BundlerConfig<I> batchingConfig, DistributedExecutorService eSvc, HazeltaskTopology topology) {
        this.hazeltaskConfig = hazeltaskConfig;
        this.batchingConfig = batchingConfig;
        this.svc = eSvc;
        this.topology = topology;
        this.batchClusterService = topology.getBatchClusterService();
    }

    public BundlerConfig<I> getBatchingExecutorServiceConfig() {
        return this.batchingConfig;
    }
    
    public boolean add(I item) {
        boolean allowAdd = true;
        for(BatchExecutorListener<I> listener : batchListeners) {
            allowAdd = allowAdd && listener.beforeAdd(item);
        }
        
        boolean didAdd = false;
        
        if(allowAdd) {
            didAdd = batchClusterService.addToBatch(item);
        }
        
        for(BatchExecutorListener<I> listener : batchListeners) {
            listener.afterAdd(item, didAdd);
        }
        
        return didAdd;
    }
    
    public DistributedExecutorService getDistributedExecutorService() {
        return null;
    }
    
    public void addServiceListener(HazeltaskServiceListener<TaskBatchingService<I>> listener) {
        this.listeners.add(listener);
    }
    
    public void startup() {
        for(HazeltaskServiceListener<TaskBatchingService<I>> listener : listeners)
            listener.onBeginStart(this);
        
        //FIXME: startup
        
        for(HazeltaskServiceListener<TaskBatchingService<I>> listener : listeners)
            listener.onEndStart(this);
    }
    
    public void addListener(BatchExecutorListener<I> listener) {
        this.batchListeners.add(listener);
    }
}
