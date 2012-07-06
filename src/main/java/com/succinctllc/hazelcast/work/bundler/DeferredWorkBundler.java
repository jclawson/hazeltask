package com.succinctllc.hazelcast.work.bundler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.succinctllc.core.concurrent.BackoffTimer;
import com.succinctllc.core.metrics.MetricNamer;
import com.succinctllc.hazelcast.util.MultiMapProxy;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;
import com.succinctllc.hazelcast.work.bundler.DeferredWorkBundlerBuilder.InternalBuilderStep3;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorService;
import com.succinctllc.hazelcast.work.metrics.PercentDuplicateRateGuage;
import com.succinctllc.hazelcast.work.metrics.TotalPercentDuplicateGuage;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.TimerContext;

/**
 * 
 * 
 * 
 * @author jclawson
 *
 * @param <T>
 */
public class DeferredWorkBundler<I> {
	private static ILogger LOGGER = Logger.getLogger(DeferredWorkBundler.class.getName());
	
	
	private static ConcurrentMap<String, DeferredWorkBundler<?>> workBundlersByTopology = new ConcurrentHashMap<String, DeferredWorkBundler<?>>();
	
    private final AtomicBoolean isStarted = new AtomicBoolean();
//    private final SetMultimap<String, I> localMultiMap;
//    private final MultiMap<String, I> hcMultiMap;
    
    private final MultiMapProxy<String, I> groupItemBuffer;
    
    private final InternalBuilderStep3<I> config;
    private final DistributedExecutorService executorService;
    private final HazelcastWorkTopology topology;
    private final IMap<String, Long> duplicatePreventionMap;
    private final MetricNamer metricNamer;
    private final MetricsRegistry metrics;
    
    private com.yammer.metrics.core.Timer itemAddTimer;
    private Meter itemActuallyAddedMeter;
    private Histogram bundleSizeHistogram;
    private Histogram flushSizeHistogram;
    private Counter numDuplicates;
    
    protected DeferredWorkBundler(InternalBuilderStep3<I> config, DistributedExecutorService svc, MetricNamer metricNamer, MetricsRegistry metrics) {
        this.config = config;
        this.topology = config.topology;
        this.metricNamer = metricNamer;
        this.metrics = metrics;
        
        if (workBundlersByTopology.putIfAbsent(topology.getName(), this) != null) { 
        	throw new IllegalArgumentException("A DistributedExecutorServiceManager already exists for the topology "
                        + topology); 
        }
        
        if(!config.localBuffering) {
        	groupItemBuffer = MultiMapProxy.<String, I>clusteredMultiMap(topology.getHazelcast().<String, I>getMultiMap(topology.createName("group-item-buffer")));
        } else {
        	groupItemBuffer = MultiMapProxy.<String, I>localMultiMap(HashMultimap.<String,I>create());
        }
        
        if(config.preventDuplicates) {
            duplicatePreventionMap = topology.getHazelcast().getMap(topology.createName("bundler-items"));
        } else {
            duplicatePreventionMap = null;
        }
                
        executorService = svc;
        
        if(metrics != null) {
        	itemAddTimer 			   = metrics.newTimer(createName("[add] Call timer"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
        	itemActuallyAddedMeter     = metrics.newMeter(createName("[add] Added to buffer"), "items added", TimeUnit.MINUTES);        	
        	bundleSizeHistogram = metrics.newHistogram(createName("[flush] Bundle size"), true);
        	flushSizeHistogram = metrics.newHistogram(createName("[flush] Flush size"), true);
        	metrics.newGauge(createName("[add] Percent duplicate rate"), new PercentDuplicateRateGuage(itemActuallyAddedMeter, itemAddTimer));
        	metrics.newGauge(createName("[add] Percent duplicates"), new TotalPercentDuplicateGuage(itemActuallyAddedMeter, itemAddTimer));
        	numDuplicates = metrics.newCounter(createName("[add] Duplicate count"));
        }
                
    }
    
    private MetricName createName(String name) {
		return metricNamer.createMetricName(
			"bundler", 
			topology.getName(), 
			"DeferredWorkBundler", 
			name
		);
	}
    
    
    
    public HazelcastWorkTopology getTopology() {
		return topology;
	}

	@SuppressWarnings("unchecked")
	public static <U> DeferredWorkBundler<U> getDeferredWorkBundler(String topology) {
        return (DeferredWorkBundler<U>) workBundlersByTopology.get(topology);
    }
    
    public boolean add(I o) {
    	TimerContext tCtx = null;
        if(itemAddTimer != null) {
        	tCtx = itemAddTimer.time();
        }
        try {
        	boolean added = tryAdd(o);
        	if(added && itemActuallyAddedMeter != null) 
        		itemActuallyAddedMeter.mark();
        	
        	if(!added && numDuplicates != null)
        		numDuplicates.inc();        	
        	
        	return added;
        } finally {
        	if(tCtx != null)
        		tCtx.stop();
        }
    }
    
    private boolean tryAdd(I o) {
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

    	return groupItemBuffer.put(group, o);
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
        	//TODO: this timer sucks... is there a better way to flush (maybe with an exponential backoff?)
        	//it would be cool to have a timer task that we can tell to run immediately with a min-limit to how often it can 
        	//be run in a period manually
        	//new Timer(buildName("bundle-flush-timer"), true)
            //    .schedule(new DeferredBundleTask<I>(this, metrics, metricNamer), config.flushTTL, Math.max(config.flushTTL/4, Math.min(4000, config.flushTTL)));
        	
            BackoffTimer timer = new BackoffTimer(buildName("bundle-flush-timer"));
            timer.schedule(new DeferredBundleTask<I>(this, metrics, metricNamer), 200, 20000, 2);
            timer.start();
            
        	executorService.startup();
        }
    }
    
    private String buildName(String postfix) {
        return topology + "-" + postfix;
    }
    
    public Map<String, Integer> getNonZeroLocalGroupSizes() {
        Set<String> localKeys = groupItemBuffer.keySet();
        
        Map<String, Integer> result =  new HashMap<String, Integer>(localKeys.size());
        for(String key : localKeys) {
            int size = groupItemBuffer.get(key).size();
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
    	int numNodes = topology.getReadyMembers().size();
        if(numNodes == 0) {
        	LOGGER.log(Level.WARNING, "I want to flush the deferred item set but no members are online to do the work!");
        	return 0;
        }
    	
    	List<I> bundle = groupItemBuffer.getAsList(group);
        
        if(bundle.size() == 0) {
            return 0;
        }
        
        //only track flushes that actually have something in them
        if(flushSizeHistogram != null)
        	flushSizeHistogram.update(bundle.size());
                
        int minBundleSize = Math.min(bundle.size(), config.minBundleSize);
        int numDividedBundles = bundle.size() / minBundleSize;
        numDividedBundles = Math.min(numDividedBundles, numNodes);
        
        int targetDividedBundleSize = Math.max((int)(bundle.size() / numDividedBundles), minBundleSize);
        targetDividedBundleSize = Math.min(config.maxBundleSize, targetDividedBundleSize);
        
        List<List<I>> partitionedBundles = Lists.partition(bundle, targetDividedBundleSize);        
        
        //bundle the objects and submit them as work
        //config.bundler.bundle(items)
        for(List<I> partitionedBundle : partitionedBundles) {
        	
        	if(bundleSizeHistogram != null)
        		bundleSizeHistogram.update(partitionedBundle.size());
        	
        	WorkBundle<I> work = config.bundler.bundle(group, partitionedBundle);
            if(this.duplicatePreventionMap != null) {
                //if we are preventing duplicates, then we need to wrap it
                work = new PreventDuplicatesWorkBundleWrapper<I>(topology.getName(), work);
            }
            
            executorService.execute(work);
            
            //TODO: is it better to just wait until the end and remove it all at once?
            //Remove work from multimap since its safe in the distributed work system
            for(I item : partitionedBundle) {
            	groupItemBuffer.remove(group, item);
            }
            
            
        }
        
        //System.out.println("Processed group "+group+".  Total flushed: "+totalFlushed.addAndGet(bundle.size()));
        
        return bundle.size();
    }
}
