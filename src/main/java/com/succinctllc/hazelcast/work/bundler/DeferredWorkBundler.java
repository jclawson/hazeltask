package com.succinctllc.hazelcast.work.bundler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;
import com.succinctllc.hazelcast.work.bundler.DeferredWorkBundlerBuilder.InternalBuilderStep3;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorService;

/**
 * 
 * 
 * 
 * @author jclawson
 *
 * @param <T>
 */
public class DeferredWorkBundler<I> {
    
	private static ConcurrentMap<String, DeferredWorkBundler<?>> workBundlersByTopology = new ConcurrentHashMap<String, DeferredWorkBundler<?>>();
	
    private final AtomicBoolean isStarted = new AtomicBoolean();
    private final SetMultimap<String, I> localMultiMap;
    private final MultiMap<String, I> hcMultiMap;
    private final InternalBuilderStep3<I> config;
    private final DistributedExecutorService executorService;
    private final HazelcastWorkTopology topology;
    private final IMap<String, Long> duplicatePreventionMap;
    
    /**
     * The flush TimerTask will run every FLUSH_TIMER_TASK_RATE milliseconds
     * to check if any of the groups need to be flushed.
     * 
     * TODO: lets put in an exponential backoff so if the bundler is getting hammered
     * it runs more often, but if its idle it runs less often
     */
    private static final long FLUSH_TIMER_TASK_RATE = 100L;
    
    protected DeferredWorkBundler(InternalBuilderStep3<I> config, DistributedExecutorService svc) {
        this.config = config;
        this.topology = config.topology;
        
        if (workBundlersByTopology.putIfAbsent(topology.getName(), this) != null) { 
        	throw new IllegalArgumentException("A DistributedExecutorServiceManager already exists for the topology "
                        + topology); 
        }
        
        if(!config.localBuffering) {
            hcMultiMap = topology.getHazelcast().getMultiMap(topology.createName("bundler"));
            localMultiMap = null;
        } else {
            hcMultiMap = null; 
            localMultiMap = Multimaps.<String,I>synchronizedSetMultimap(HashMultimap.<String,I>create());
        }
        
        if(config.preventDuplicates) {
            duplicatePreventionMap = topology.getHazelcast().getMap(topology.createName("bundler-items"));
        } else {
            duplicatePreventionMap = null;
        }
                
        executorService = svc;
                
    }
    
    @SuppressWarnings("unchecked")
	public static <U> DeferredWorkBundler<U> getDeferredWorkBundler(String topology) {
        return (DeferredWorkBundler<U>) workBundlersByTopology.get(topology);
    }
    
    public boolean add(I o) {
        String group = config.partitioner.getItemGroup(o);
        boolean added = false; 
        
        if(duplicatePreventionMap != null) {
            if(duplicatePreventionMap.putIfAbsent(
                    config.partitioner.getItemId(o), 
                    System.currentTimeMillis(), 
                    this.config.maxDuplicatePreventionTTL, 
                    TimeUnit.MILLISECONDS) 
                != null) {
                return false;
            }
        }
        
        if(localMultiMap != null) {
            added = localMultiMap.put(group, o);
            //checkAndDoFlush(group, localMultiMap.get(group).size());
        } else {
        	added = hcMultiMap.put(group, o);
        	//checkAndDoFlush(group, hcMultiMap.get(group).size());
        }
        
        return added;
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
    public void startup() {
        if(!isStarted.getAndSet(true)) {
        	new Timer(buildName("bundle-flush-timer"), true)
                .schedule(new DeferredBundleTask<I>(this), config.flushTTL, FLUSH_TIMER_TASK_RATE);
        	
        	executorService.startup();
        }
    }
    
    private String buildName(String postfix) {
        return topology + "-" + postfix;
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
    
    protected void removePreventDuplicateItems(WorkBundle<I> work) {
        if(this.duplicatePreventionMap != null) {
            for(I item : work.getItems()) {
                String id = config.partitioner.getItemId(item);
                this.duplicatePreventionMap.remove(id);
            }
        }
    }
    
    /**
     * TODO: Could potentially want to create HUGE bundles.
     * 
     * Note: caller should ensure that this node owns the group partition
     * @param group
     * @return
     */
    protected int flush(String group) {
        List<I> bundle;
        if(localMultiMap != null) {
            bundle = new ArrayList<I>(localMultiMap.get(group));
        } else {
            bundle = new ArrayList<I>(hcMultiMap.get(group)); //this is actually a Set
        }
        
        if(bundle == null || bundle.size() == 0) {
            return 0;
        }
        
        int numNodes = topology.getHazelcast().getCluster().getMembers().size();
        int minBundleSize = Math.min(bundle.size(), config.minBundleSize);
        int numDividedBundles = bundle.size() / minBundleSize;
        numDividedBundles = Math.min(numDividedBundles, numNodes);
        
        int targetDividedBundleSize = Math.max((int)(bundle.size() / numDividedBundles), minBundleSize);
        targetDividedBundleSize = Math.min(config.maxBundleSize, targetDividedBundleSize);
        
        List<List<I>> partitionedBundles = Lists.partition(bundle, targetDividedBundleSize);        
        
        //bundle the objects and submit them as work
        //config.bundler.bundle(items)
        for(List<I> partitionedBundle : partitionedBundles) {
            WorkBundle<I> work = config.bundler.bundle(group, partitionedBundle);
            if(this.duplicatePreventionMap != null) {
                //if we are preventing duplicates, then we need to wrap it
                work = new PreventDuplicatesWorkBundleWrapper<I>(topology.getName(), work);
            }
            
            executorService.execute(work);
            
            //TODO: is it better to just wait until the end and remove it all at once?
            //remove work from multimap since its safe in the distributed work system
            if(localMultiMap != null) {
                for(I item : partitionedBundle) {
                    localMultiMap.remove(group, item);
                }
            } else {
                for(I item : partitionedBundle) {
                    hcMultiMap.remove(group, item);
                }
            }
        }
        
        
        return bundle.size();
    }
}
