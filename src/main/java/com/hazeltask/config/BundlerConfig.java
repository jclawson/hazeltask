package com.hazeltask.config;

import com.hazeltask.batch.BatchKeyAdapter;
import com.hazeltask.batch.DefaultBatchKeyAdapter;
import com.hazeltask.batch.IBatchFactory;

public class BundlerConfig<I> {
    private int                  flushSize                     = 100;
    private int                  minBundleSize                 = 25;
    private int                  maxBundleSize                 = 100;
    private long                 flushTTL                      = 10000;
    private boolean              localBuffering                = false;
    private boolean              preventDuplicates             = false;
    private long                 duplicatePreventionExpireTime = 3600000;      // 20 minutes
    protected BatchKeyAdapter<I> batchKeyAdapter               = new DefaultBatchKeyAdapter<I>();
    private final IBatchFactory<I>     bundler;

    public BundlerConfig(IBatchFactory<I> bundler) {
        this.bundler = bundler;
    }
    
    public BundlerConfig<I> withFlushSize(int flushSize) {
        this.flushSize = flushSize;
        return this;
    }

    public BundlerConfig<I> withMinBundleSize(int minBundleSize) {
        this.minBundleSize = minBundleSize;
        return this;
    }

    public BundlerConfig<I> withMaxBundleSize(int maxBundleSize) {
        this.maxBundleSize = maxBundleSize;
        return this;
    }

    public BundlerConfig<I> withFlushTTL(long flushTTL) {
        this.flushTTL = flushTTL;
        return this;
    }

    public BundlerConfig<I> withLocalBuffering(boolean localBuffering) {
        this.localBuffering = localBuffering;
        return this;
    }

    /**
     * NOTE: this slows things down a lot of you add tons of items really fast.  It 
     * is best to find a smarter way to do duplicate prevention.  Or don't do it at all.
     * 
     * @param preventDuplicates
     * @return
     */
    public BundlerConfig<I> withPreventDuplicates(boolean preventDuplicates) {
        this.preventDuplicates = preventDuplicates;
        return this;
    }

    public BundlerConfig<I> withDuplicatePreventionExpireTime(long duplicatePreventionExpireTime) {
        this.duplicatePreventionExpireTime = duplicatePreventionExpireTime;
        return this;
    }

    public BundlerConfig<I> withBatchKeyAdapter(BatchKeyAdapter<I> batchKeyAdapter) {
        this.batchKeyAdapter = batchKeyAdapter;
        return this;
    }
    
    
    public int getFlushSize() {
        return flushSize;
    }

    public int getMinBundleSize() {
        return minBundleSize;
    }

    public int getMaxBundleSize() {
        return maxBundleSize;
    }

    public long getFlushTTL() {
        return flushTTL;
    }

    public boolean isLocalBuffering() {
        return localBuffering;
    }

    public boolean isPreventDuplicates() {
        return preventDuplicates;
    }

    public long getDuplicatePreventionExpireTime() {
        return duplicatePreventionExpireTime;
    }

    public BatchKeyAdapter<I> getBatchKeyAdapter() {
        return batchKeyAdapter;
    }

    public IBatchFactory<I> getBundler() {
        return bundler;
    }
    
    

}
