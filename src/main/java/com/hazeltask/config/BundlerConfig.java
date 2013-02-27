package com.hazeltask.config;

import java.io.Serializable;

import com.hazeltask.batch.BatchKeyAdapter;
import com.hazeltask.batch.DefaultBatchKeyAdapter;
import com.hazeltask.batch.IBatchFactory;
import com.hazeltask.batch.TaskBatch;

public class BundlerConfig<I, ITEM_ID extends Serializable, ID extends Serializable, GROUP extends Serializable> {
    private int                  flushSize                     = 100;
    private int                  minBundleSize                 = 25;
    private int                  maxBundleSize                 = 100;
    private long                 flushTTL                      = 10000;
    private boolean              localBuffering                = false;
    private boolean              preventDuplicates             = false;
    private long                 duplicatePreventionExpireTime = 3600000;      // 20 minutes
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected BatchKeyAdapter<I, ? extends TaskBatch<I, ITEM_ID, GROUP>, ITEM_ID, ID, GROUP> batchKeyAdapter               = new DefaultBatchKeyAdapter();
    private final IBatchFactory<I, ID, GROUP>     bundler;

    public <G extends Serializable> BundlerConfig(IBatchFactory<I, ID, GROUP> bundler) {
        this.bundler = bundler;
    }
    
    public BundlerConfig<I, ITEM_ID, ID, GROUP> withFlushSize(int flushSize) {
        this.flushSize = flushSize;
        return this;
    }

    public BundlerConfig<I, ITEM_ID, ID, GROUP> withMinBundleSize(int minBundleSize) {
        this.minBundleSize = minBundleSize;
        return this;
    }

    public BundlerConfig<I, ITEM_ID, ID, GROUP> withMaxBundleSize(int maxBundleSize) {
        this.maxBundleSize = maxBundleSize;
        return this;
    }

    public BundlerConfig<I, ITEM_ID, ID, GROUP> withFlushTTL(long flushTTL) {
        this.flushTTL = flushTTL;
        return this;
    }

    public BundlerConfig<I, ITEM_ID, ID, GROUP> withLocalBuffering(boolean localBuffering) {
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
    public BundlerConfig<I, ITEM_ID, ID, GROUP> withPreventDuplicates(boolean preventDuplicates) {
        this.preventDuplicates = preventDuplicates;
        return this;
    }

    public BundlerConfig<I, ITEM_ID, ID, GROUP> withDuplicatePreventionExpireTime(long duplicatePreventionExpireTime) {
        this.duplicatePreventionExpireTime = duplicatePreventionExpireTime;
        return this;
    }

    public BundlerConfig<I, ITEM_ID, ID, GROUP> withBatchKeyAdapter(BatchKeyAdapter<I, ? extends TaskBatch<I, ITEM_ID, GROUP>, ITEM_ID, ID, GROUP> batchKeyAdapter) {
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

    public BatchKeyAdapter<I, ? extends TaskBatch<I, ITEM_ID, GROUP>, ITEM_ID, ID, GROUP> getBatchKeyAdapter() {
        return batchKeyAdapter;
    }

    /**
     * I cast this to Serializable because for all intents and purposes, thats
     * all I need to know for the internals.
     * @return
     */
    public IBatchFactory<I, ID, GROUP> getBundler() {
        return bundler;
    }
    
    

}
