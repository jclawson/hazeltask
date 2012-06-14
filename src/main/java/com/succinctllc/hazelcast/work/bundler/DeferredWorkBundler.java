package com.succinctllc.hazelcast.work.bundler;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.MultiMap;
import com.succinctllc.hazelcast.work.HazelcastWorkManager;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;
import com.succinctllc.hazelcast.work.bundler.DeferredWorkBundlerBuilder.InternalBuilderStep3;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorServiceManager;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorService;

/**
 * 
 * 
 * 
 * @author jclawson
 *
 * @param <T>
 */
public class DeferredWorkBundler<T> {
    
	private static ConcurrentMap<String, DeferredWorkBundler<?>> workBundlersByTopology = new ConcurrentHashMap<String, DeferredWorkBundler<?>>();
	
    private final AtomicBoolean isStarted = new AtomicBoolean();
    private final SetMultimap<String, T> localMultiMap;
    private final MultiMap<String, T> hcMultiMap;
    private final InternalBuilderStep3<T> config;
    private final DistributedExecutorService executorService;
    private final HazelcastWorkTopology topology;
    //private final ExecutorService asyncFlushExecutor;
    
    /**
     * The flush TimerTask will run every FLUSH_TIMER_TASK_RATE milliseconds
     * to check if any of the groups need to be flushed.
     * 
     * TODO: lets put in an exponential backoff so if the bundler is getting hammered
     * it runs more often, but if its idle it runs less often
     */
    private static final long FLUSH_TIMER_TASK_RATE = 100L;
    
    protected DeferredWorkBundler(InternalBuilderStep3<T> config) {
        this.config = config;
        this.topology = config.topology;
        
        if (workBundlersByTopology.putIfAbsent(topology.getName(), this) != null) { 
        	throw new IllegalArgumentException("A DistributedExecutorServiceManager already exists for the topology "
                        + config.topology); 
        }
        
        if(!config.localBuffering) {
            hcMultiMap = topology.getHazelcast().getMultiMap(topology.createName("bundler"));
            localMultiMap = null;
        } else {
            hcMultiMap = null; 
            localMultiMap = Multimaps.<String,T>synchronizedSetMultimap(HashMultimap.<String,T>create());
        }
                
        executorService = HazelcastWorkManager.getDistributedExecutorService(config.topology.getName());
        
//        asyncFlushExecutor = new ThreadPoolExecutor(1, 1,
//                        0L, TimeUnit.MILLISECONDS,
//                        new SetLinkedBlockingQueue<Runnable>());        
    }
    
    @SuppressWarnings("unchecked")
	public static <U> DeferredWorkBundler<U> getDeferredWorkBundler(String topology) {
        return (DeferredWorkBundler<U>) workBundlersByTopology.get(topology);
    }
    
   
//NOTE: I think i would rather have the FlushTask run more often to flush things
//    private void checkAndDoFlush(String group, int size) {
//    	if(size >= config.flushSize) {
//    		asyncFlushExecutor.execute(new AsyncFlush<T>(config.topology, group));
//    	}
//    }
    
    public boolean add(T o) {
        String group = config.partitioner.getGroup(o);
        if(localMultiMap != null) {
            boolean added = localMultiMap.put(group, o);
            //checkAndDoFlush(group, localMultiMap.get(group).size());
            return added;
        } else {
        	boolean added = hcMultiMap.put(group, o);
        	//checkAndDoFlush(group, hcMultiMap.get(group).size());
        	return added;
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
                .schedule(new DeferredBundleTask<T>(this), config.flushTTL, FLUSH_TIMER_TASK_RATE);
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
    
    public static class AsyncFlush<T> implements Runnable, Serializable {
		private static final long serialVersionUID = 1L;
		private String group;
		private String topology;
		
		public AsyncFlush(String topology, String group) {
			this.group = group;
			this.topology = topology;
		}
		
		public void run() {
			DeferredWorkBundler<T> bundler = DeferredWorkBundler.getDeferredWorkBundler(topology);
			bundler.flush(group);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((group == null) ? 0 : group.hashCode());
			result = prime * result
					+ ((topology == null) ? 0 : topology.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			AsyncFlush other = (AsyncFlush) obj;
			if (group == null) {
				if (other.group != null)
					return false;
			} else if (!group.equals(other.group))
				return false;
			if (topology == null) {
				if (other.topology != null)
					return false;
			} else if (!topology.equals(other.topology))
				return false;
			return true;
		}
		
		
    }
}
