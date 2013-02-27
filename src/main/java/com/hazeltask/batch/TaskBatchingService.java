package com.hazeltask.batch;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import com.google.common.collect.Lists;
import com.hazelcast.logging.ILogger;
import com.hazeltask.HazeltaskServiceListener;
import com.hazeltask.HazeltaskTopology;
import com.hazeltask.ServiceListenable;
import com.hazeltask.config.BundlerConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.executor.DistributedExecutorService;


//TODO: add better generics...
public class TaskBatchingService<I, ITEM_ID extends Serializable, ID extends Serializable, GROUP extends Serializable> implements ServiceListenable<TaskBatchingService<I,ITEM_ID, ID,GROUP>> {
    
    private final BundlerConfig<I,ITEM_ID, ID,GROUP> batchingConfig;
    private final DistributedExecutorService svc;
    private final HazeltaskTopology topology;
    private final IBatchClusterService<I,ITEM_ID,GROUP> batchClusterService;
    private CopyOnWriteArrayList<HazeltaskServiceListener<TaskBatchingService<I,ITEM_ID, ID, GROUP>>> listeners = new CopyOnWriteArrayList<HazeltaskServiceListener<TaskBatchingService<I,ITEM_ID, ID,GROUP>>>();
    private final CopyOnWriteArrayList<BatchExecutorListener<I>> batchListeners = new CopyOnWriteArrayList<BatchExecutorListener<I>>();
    private final ILogger LOGGER;
    
    @SuppressWarnings("unchecked")
    public TaskBatchingService(HazeltaskConfig hazeltaskConfig, DistributedExecutorService eSvc, HazeltaskTopology topology) {
        this.batchingConfig = hazeltaskConfig.getBundlerConfig();
        this.svc = eSvc;
        this.topology = topology;
        this.batchClusterService = topology.getBatchClusterService();
        LOGGER = topology.getLoggingService().getLogger(TaskBatchingService.class.getName());
    }

    public BundlerConfig<I,ITEM_ID, ID,GROUP> getBatchingExecutorServiceConfig() {
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
    
    public BatchingContext<I> createBatch() {
        return new BatchingContext<I>(svc, batchingConfig);
    }
    
    public DistributedExecutorService getDistributedExecutorService() {
        return svc;
    }
    
    public void addServiceListener(HazeltaskServiceListener<TaskBatchingService<I,ITEM_ID,ID,GROUP>> listener) {
        this.listeners.add(listener);
    }
    
    public void startup() {
        for(HazeltaskServiceListener<TaskBatchingService<I,ITEM_ID, ID,GROUP>> listener : listeners)
            listener.onBeginStart(this);
        
        svc.startup();
        
        for(HazeltaskServiceListener<TaskBatchingService<I,ITEM_ID,ID,GROUP>> listener : listeners)
            listener.onEndStart(this);
    }
    
    public void addListener(BatchExecutorListener<I> listener) {
        this.batchListeners.add(listener);
    }
    
    protected int flush(GROUP group) {
        int numNodes = topology.getReadyMembers().size();
        if (numNodes == 0) {
            LOGGER.log(Level.WARNING,
                    "I want to flush the deferred item set but no members are online to do the work!");
            return 0;
        }
    
        List<I> items = batchClusterService.getItems(group);
        if (items == null || items.size() == 0) { return 0; }
        
      int minBundleSize = Math.min(items.size(), batchingConfig.getMinBundleSize());
      int numDividedBundles = items.size() / minBundleSize;
      numDividedBundles = Math.min(numDividedBundles, numNodes);
      
      int targetDividedBundleSize = Math.max((int)(items.size() / numDividedBundles), minBundleSize);
      targetDividedBundleSize = Math.min(batchingConfig.getMaxBundleSize(), targetDividedBundleSize);
      
      List<List<I>> partitionedBundles = Lists.partition(items, targetDividedBundleSize);        
      
      //bundle the objects and submit them as work
      //config.bundler.bundle(items)
      for(List<I> partitionedBundle : partitionedBundles) {
          
//          if(bundleSizeHistogram != null)
//              bundleSizeHistogram.update(partitionedBundle.size());
          
          IBatchFactory<I,ID,GROUP> batcher = batchingConfig.getBundler();
          TaskBatch<I,ID,GROUP> task = batcher.createBatch(group, partitionedBundle);
          
          svc.execute(task);   
      }
      
      batchClusterService.removeItems(group, items);
      
      //System.out.println("Processed group "+group+".  Total flushed: "+totalFlushed.addAndGet(bundle.size()));
      
      return items.size();
    }
    
    public Map<GROUP, Integer> getNonZeroLocalGroupSizes() {
        return batchClusterService.getNonZeroLocalGroupSizes();
    }
}