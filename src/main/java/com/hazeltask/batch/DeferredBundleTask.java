package com.hazeltask.batch;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.hazeltask.batch.TaskBatchingService;
import com.hazeltask.config.BundlerConfig;
import com.hazeltask.core.concurrent.BackoffTimer.BackoffTask;
import com.hazeltask.core.metrics.MetricNamer;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 * This TimerTask runs every <code>flushTTL</code> milliseconds in order 
 * to bundle up the groups into work to submit to the distributed executor
 * service
 * 
 * @author jclawson
 *
 * @param <T>
 */
public class DeferredBundleTask<T> extends BackoffTask {
    private final TaskBatchingService<T> deferredWorkBundler;
    private final BundlerConfig<T> batchConfig;
    
    private final Map<String, Long> lastFlushedTimes = new HashMap<String, Long>();
    //private final MetricNamer metricNamer;
    
    private Timer bundlerTimer;
   
    
    public DeferredBundleTask(BundlerConfig<T> batchingConfig, TaskBatchingService<T> deferredWorkBundler) {
        this.deferredWorkBundler = deferredWorkBundler;
       // this.metricNamer = metricNamer;
        this.batchConfig = batchingConfig;
 
//FIXME: add this metric
//        if(metrics != null) {
//        	bundlerTimer = metrics.newTimer(createName("Bundle timer"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);        	
//        }        
    }
    
//    private MetricName createName(String name) {
//		return metricNamer.createMetricName(
//			"hazelcast-work", 
//			deferredWorkBundler.getTopology().getName(), 
//			"DeferredBundleTask", 
//			name
//		);
//	}
    
    /**
     * This method also updates the next flush time
     * @param group
     * @return
     */
    private boolean shouldTTLFlush(String group) {
        Long lastFlushed = lastFlushedTimes.get(group);
        long currentTime = System.currentTimeMillis();
        if(lastFlushed == null || currentTime - lastFlushed > batchConfig.getFlushTTL()) {
            lastFlushedTimes.put(group, currentTime);
            return true;
        } else {
            return false;
        }
    }

    
    @Override
    public boolean execute() {
        boolean flushed = false;
        TimerContext tCtx = null;
        if(bundlerTimer != null)
        	tCtx = bundlerTimer.time();
    	
        try {
	    	Map<String, Integer> sizes = deferredWorkBundler.getNonZeroLocalGroupSizes();
	        int flushSize = batchConfig.getFlushSize();
	        for(Entry<String, Integer> entry : sizes.entrySet()) {
	            if(entry.getValue() >= flushSize) {
	                String group = entry.getKey();
	                lastFlushedTimes.put(group, System.currentTimeMillis());
	                int numFlushed = deferredWorkBundler.flush(group);
	                //TODO: historgram this
	                flushed = true;
	            } else if (shouldTTLFlush(entry.getKey())) {
	                int numFlushed = deferredWorkBundler.flush(entry.getKey());
	                //TODO: historgram this
	                flushed = true;
	            }
	        }
        } finally {
        	if(tCtx != null)
        		tCtx.stop();
        }
        
        return flushed;
    }
}
