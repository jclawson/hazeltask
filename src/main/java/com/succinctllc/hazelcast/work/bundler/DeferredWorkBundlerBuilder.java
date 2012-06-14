package com.succinctllc.hazelcast.work.bundler;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.core.HazelcastInstance;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;

/**
 * Many times its more efficient to do work if they are grouped into a small
 * batch. for example, if you need to perform a lot of SELECT queries its more
 * efficient to SELECT 100 items 1 time than it is to SELECT 1 item 100 times.
 * 
 * This abstraction to the Partitioned Executor Service will help you do this
 * bundling
 * 
 * FIXME: require providing an instance of DistributedExecutorServiceManager
 * because this is a dependency that must be created before creating this guy
 * 
 * @author jclawson
 * 
 */
public class DeferredWorkBundlerBuilder<T> {
    
    public static AtomicInteger atomicServiceIndex = new AtomicInteger(1);
    
    public static class InternalBuilderStep1<T> {
        private final HazelcastInstance hazelcast;

        public InternalBuilderStep1(HazelcastInstance hazelcast) {
            this.hazelcast = hazelcast;
        }

        /**
         * We must be able to, given a piece of work, identify its group
         * 
         * @param adapter
         * @return
         */
        public InternalBuilderStep2<T> withPartitioner(Grouper partitioner) {
            return new InternalBuilderStep2<T>(hazelcast, partitioner);
        }
    }

    public static class InternalBuilderStep2<T> {
        private HazelcastInstance hazelcast;
        private Grouper       partitioner;

        public InternalBuilderStep2(HazelcastInstance hazelcast, Grouper partitioner) {
            this.hazelcast = hazelcast;
            this.partitioner = partitioner;
        }

        public InternalBuilderStep3<T> withBundler(Bundler<T> bundler) {
            return new InternalBuilderStep3<T>(hazelcast, partitioner, bundler);
        }

    }

    public static class InternalBuilderStep3<T> {
        protected HazelcastInstance hazelcast;
        protected Grouper       partitioner;
        protected Bundler<T>        bundler;
        protected int               flushSize;
        protected int               minBundleSize;
        protected int               maxBundleSize;
        protected long              flushTTL;
        protected boolean           localBuffering;
        protected HazelcastWorkTopology            topology;

        public InternalBuilderStep3(HazelcastInstance hazelcast, Grouper partitioner, Bundler<T> bundler) {
            this.hazelcast          = hazelcast;
            this.partitioner        = partitioner;
            this.bundler            = bundler;

            // defaults ------
            this.flushSize          = 200;
            this.minBundleSize      = 50;
            this.maxBundleSize      = 100;
            this.flushTTL           = 10000;
            this.localBuffering     = false;
            this.topology           = null;
        }

        public InternalBuilderStep3<T> withFlushSize(int flushSize) {
            this.flushSize = flushSize;
            return this;
        }

        public InternalBuilderStep3<T> withMinBundleSize(int minBundleSize) {
            this.minBundleSize = minBundleSize;
            return this;
        }
        
        public InternalBuilderStep3<T> withMaxBundleSize(int maxBundleSize) {
            this.maxBundleSize = maxBundleSize;
            return this;
        }

        public InternalBuilderStep3<T> withFlushTTL(long flushTTL) {
            this.flushTTL = flushTTL;
            return this;
        }
        
        public InternalBuilderStep3<T> withTopology(HazelcastWorkTopology topology) {
            this.topology = topology;
            return this;
        }
        
        /**
         * By default we do clustered buffering in hazelcast to prevent losing work
         * If you would rather things go faster, and take the chance of losing work
         * if nodes go down, then use local buffering
         * 
         * @return
         */
        public InternalBuilderStep3<T> withLocalBufferingOnly() {
            this.localBuffering = true;
            return this;
        }
        
        public DeferredWorkBundler<T> build() {
        	if(this.topology == null)
        		withTopology(HazelcastWorkTopology.getDefault(hazelcast));
        	return new DeferredWorkBundler<T>(this);
        }
    }
}
