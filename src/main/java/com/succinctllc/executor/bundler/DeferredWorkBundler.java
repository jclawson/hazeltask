package com.succinctllc.executor.bundler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.MultiMap;
import com.succinctllc.executor.DistributedExecutorServiceManager;
import com.succinctllc.executor.TaskExecutorService;
import com.succinctllc.executor.bundler.DeferredWorkBundlerBuilder.InternalBuilderStep3;

/**
 * 
 * TODO: maybe add in a singleThreadExecutorService to handle flushing a group
 * when add is called.  Should also be set to discard the task if its blocking queue
 * is full.  This would help flush the groups more often when they fill up quickly.
 *   - should be handled by a local listener in hazelcast to ensure it works locally only
 *   - can be handled in-line with add() the local multi map
 * 
 * 
 * @author jclawson
 *
 * @param <T>
 */
public class DeferredWorkBundler<T> {
    
    private final AtomicBoolean isStarted = new AtomicBoolean();
    private final SetMultimap<String, T> localMultiMap;
    private final MultiMap<String, T> hcMultiMap;
    private final InternalBuilderStep3<T> config;
    private final TaskExecutorService executorService;
    
    protected DeferredWorkBundler(InternalBuilderStep3<T> config) {
        this.config = config;
        if(!config.localBuffering) {
            hcMultiMap = config.hazelcast.getMultiMap(config.topology);
            localMultiMap = null;
        } else {
            hcMultiMap = null; 
            localMultiMap = Multimaps.<String,T>synchronizedSetMultimap(HashMultimap.<String,T>create());
        }
        
        DistributedExecutorServiceManager mgr =
                DistributedExecutorServiceManager.getDistributedExecutorServiceManager(config.topology);
        
        executorService = mgr.getDistributedExecutorService();
    }
    
    public boolean add(T o) {
        String group = config.partitioner.getGroup(o);
        if(localMultiMap != null) {
            return localMultiMap.put(group, o);
        } else {
            return hcMultiMap.put(group, o);
        }
    }
    
    public long getFlushTTL() {
        return config.flushTTL;
    }
    
    public int getFlushSize() {
        return config.flushSize;
    }
    
    /**
     * In order to start bundling and submitting work items to be worked on you 
     * must make sure to call start()
     * 
     */
    public void start() {
        if(!isStarted.getAndSet(true)) {
            new Timer(buildName("bundle-flush-timer"), true)
                .schedule(new DeferredBundleTask<T>(this), config.flushTTL, config.flushTTL);
        }
    }
    
    private String buildName(String postfix) {
        return config.topology + "-" + postfix;
    }
    
    public Map<String, Integer> getNonZeroLocalGroupSizes() {
        Set<String> localKeys;
        if(localMultiMap != null) {
            localKeys = localMultiMap.keySet();            
        } else {
            localKeys = hcMultiMap.localKeySet();
        }
        
        Map<String, Integer> result =  new HashMap<String, Integer>(localKeys.size());
        for(String key : localKeys) {
            int size = (localMultiMap != null) 
                        ? localMultiMap.get(key).size() 
                        : hcMultiMap.get(key).size();
            if(size > 0)
                result.put(key, size);
        }
        return result;
    }
    /**
     * TODO: Could potentially want to create HUGE bundles.
     * 
     * Note: caller should ensure that this node owns the group partition
     * @param group
     * @return
     */
    protected int flush(String group) {
        List<T> bundle;
        if(localMultiMap != null) {
            bundle = new ArrayList<T>(localMultiMap.get(group));
        } else {
            bundle = new ArrayList<T>(hcMultiMap.get(group)); //this is actually a Set
        }
        
        if(bundle == null || bundle.size() == 0) {
            return 0;
        }
        
        int numNodes = config.hazelcast.getCluster().getMembers().size();
        int minBundleSize = Math.min(bundle.size(), config.minBundleSize);
        int numDividedBundles = bundle.size() / minBundleSize;
        numDividedBundles = Math.min(numDividedBundles, numNodes);
        
        int targetDividedBundleSize = Math.max((int)(bundle.size() / numDividedBundles), minBundleSize);
        targetDividedBundleSize = Math.min(config.maxBundleSize, targetDividedBundleSize);
        
        List<List<T>> partitionedBundles = Lists.partition(bundle, targetDividedBundleSize);        
        
        //bundle the objects and submit them as work
        //config.bundler.bundle(items)
        for(List<T> partitionedBundle : partitionedBundles) {
            Runnable work = config.bundler.bundle(partitionedBundle);
            executorService.execute(work);
            
            //TODO: is it better to just wait until the end and remove it all at once?
            //remove work from multimap since its safe in the distributed work system
            if(localMultiMap != null) {
                for(T item : partitionedBundle) {
                    localMultiMap.remove(group, item);
                }
            } else {
                for(T item : partitionedBundle) {
                    hcMultiMap.remove(group, item);
                }
            }
        }
        
        
        return bundle.size();
    }
}
