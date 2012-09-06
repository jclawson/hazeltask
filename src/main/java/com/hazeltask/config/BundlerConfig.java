package com.hazeltask.config;

import com.hazeltask.batch.BatchKeyAdapter;
import com.hazeltask.batch.Bundler;
import com.hazeltask.batch.DefaultBatchKeyAdapter;
import com.hazeltask.core.concurrent.collections.router.ListRouterFactory;

public class BundlerConfig<I> {
    private int                  flushSize                     = 100;
    private int                  minBundleSize                 = 25;
    private int                  maxBundleSize                 = 100;
    private long                 flushTTL                      = 10000;
    private boolean              localBuffering                = false;
    private boolean              preventDuplicates             = false;
    private long                 duplicatePreventionExpireTime = 3600000;      // 20 minutes
    protected BatchKeyAdapter<I> batchKeyAdapter               = new DefaultBatchKeyAdapter<I>();
    private final Bundler<I>     bundler;
//    private BloomFilterConfig    preventDuplicatesBloomFilter  = null;
    
    private ExecutorConfig executorConfig = new ExecutorConfig();

    public BundlerConfig(Bundler<I> bundler) {
        this.bundler = bundler;
        /* We don't use futures with the bundling service 
         */
        this.executorConfig.disableFutureSupport();
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
    
//    /**
//     * TODO: how can we implement a distributed bloom filter that is fast?
//     * 
//     * @param config
//     * @return
//     */
//    public BundlerConfig<I> withPreventDuplicatesUsingBloomFilter(BloomFilterConfig config) {
//        this.preventDuplicatesBloomFilter = config;
//        this.preventDuplicates = true;
//        return this;
//    }

    public BundlerConfig<I> withDuplicatePreventionExpireTime(long duplicatePreventionExpireTime) {
        this.duplicatePreventionExpireTime = duplicatePreventionExpireTime;
        return this;
    }

    @SuppressWarnings("rawtypes")
    public BundlerConfig<I> withBatchKeyAdapter(BatchKeyAdapter batchKeyAdapter) {
        this.batchKeyAdapter = batchKeyAdapter;
        this.executorConfig.withWorkIdAdapter(batchKeyAdapter);
        return this;
    }
    
    //ExecutorConfig fluent methods
    public BundlerConfig<I> withAcknowlegeWorkSubmission(boolean acknowlegeWorkSubmission) {
        this.executorConfig.withAcknowlegeWorkSubmission(acknowlegeWorkSubmission);
        return this;
    }
    
    public BundlerConfig<I> acknowlegeWorkSubmission() {
        this.executorConfig.acknowlegeWorkSubmission();
        return this;
    }
    
    public BundlerConfig<I> withDisableWorkers(boolean disableWorkers) {
        this.executorConfig.withDisableWorkers(disableWorkers);
        return this;
    }
    
    public BundlerConfig<I> disableWorkers() {
        this.executorConfig.disableWorkers();
        return this;
    }
    
    public BundlerConfig<I> withThreadCount(int threadCount) {
        this.executorConfig.withThreadCount(threadCount);
        return this;
    }
    
    /**
     * @see ExecutorConfig.withAutoStart
     */
    public BundlerConfig<I> withAutoStart(boolean autoStart) {
        this.executorConfig.withAutoStart(autoStart);
        return this;
    }
    
    /**
     * @see ExecutorConfig.withAutoStart
     */
    public BundlerConfig<I> disableAutoStart() {
        this.executorConfig.disableAutoStart();
        return this;
    }
    
    public BundlerConfig<I> withMemberRouterFactory(ListRouterFactory factory) {
        this.executorConfig.withMemberRouterFactory(factory);
        return this;
    }
    
    //End ExecutorConfig fluent methods
    
    public ExecutorConfig getExecutorConfig() {
        return this.executorConfig;
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

    public Bundler<I> getBundler() {
        return bundler;
    }
    
    

}
