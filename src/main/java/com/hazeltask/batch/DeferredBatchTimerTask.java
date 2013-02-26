package com.hazeltask.batch;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.hazeltask.config.BundlerConfig;
import com.hazeltask.core.concurrent.BackoffTimer.BackoffTask;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 * This TimerTask runs every <code>flushTTL</code> milliseconds in order 
 * to bundle up the groups into work to submit to the distributed executor
 * service
 * 
 * TODO: add better generics
 * 
 * @author jclawson
 *
 * @param <T>
 */
public class DeferredBatchTimerTask<T> extends BackoffTask {
    private final TaskBatchingService deferredTaskBundler;
    private final BundlerConfig<T,?,?,?> batchConfig;
    
    private final Map<Serializable, Long> lastFlushedTimes = new HashMap<Serializable, Long>();
    //private final MetricNamer metricNamer;
    
    private final Timer bundlerTimer;
    private final Histogram batchSizeHistogram;
    
    public DeferredBatchTimerTask(BundlerConfig<T,?,?,?> batchingConfig, TaskBatchingService<T,?,?> deferredTaskBundler, BatchMetrics metrics) {
        this.deferredTaskBundler = deferredTaskBundler;
        this.batchConfig = batchingConfig;
        this.bundlerTimer = metrics.getBatchBundleTimer().getMetric();   
        this.batchSizeHistogram = metrics.getBatchSizeHistogram().getMetric();
    }

    
    /**
     * This method also updates the next flush time
     * @param group
     * @return
     */
    private boolean shouldTTLFlush(Serializable group) {
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
	    	Map<Serializable, Integer> sizes = deferredTaskBundler.getNonZeroLocalGroupSizes();
	        int flushSize = batchConfig.getFlushSize();
	        for(Entry<Serializable, Integer> entry : sizes.entrySet()) {
	            if(entry.getValue() >= flushSize) {
	                Serializable group = entry.getKey();
	                lastFlushedTimes.put(group, System.currentTimeMillis());
	                int numFlushed = deferredTaskBundler.flush(group);
	                batchSizeHistogram.update(numFlushed);
	                flushed = true;
	            } else if (shouldTTLFlush(entry.getKey())) {
	                int numFlushed = deferredTaskBundler.flush(entry.getKey());
	                batchSizeHistogram.update(numFlushed);
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
