package com.succinctllc.hazelcast.work.bundler;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;

/**
 * This TimerTask runs every <code>flushTTL</code> milliseconds in order 
 * to bundle up the groups into work to submit to the distributed executor
 * service
 * 
 * @author jclawson
 *
 * @param <T>
 */
public class DeferredBundleTask<T> extends TimerTask {
    private final DeferredWorkBundler<T> deferredWorkBundler;
    
    private final Map<String, Long> lastFlushedTimes = new HashMap<String, Long>();
    
    public DeferredBundleTask(DeferredWorkBundler<T> deferredWorkBundler) {
        this.deferredWorkBundler = deferredWorkBundler;
    }
    
    private boolean shouldTTLFlush(String group) {
        Long lastFlushed = lastFlushedTimes.get(group);
        long currentTime = System.currentTimeMillis();
        if(lastFlushed == null || currentTime - lastFlushed > this.deferredWorkBundler.getFlushTTL()) {
            lastFlushedTimes.put(group, currentTime);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void run() {
        Map<String, Integer> sizes = this.deferredWorkBundler.getNonZeroLocalGroupSizes();
        int flushSize = this.deferredWorkBundler.getFlushSize();
        for(Entry<String, Integer> entry : sizes.entrySet()) {
            if(entry.getValue() >= flushSize) {
                String group = entry.getKey();
                lastFlushedTimes.put(group, System.currentTimeMillis());
                deferredWorkBundler.flush(group);
            } else if (shouldTTLFlush(entry.getKey())) {
                deferredWorkBundler.flush(entry.getKey());
            }
        }
    }
}
