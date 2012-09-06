package com.succinctllc.hazelcast.work.bundler;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazeltask.batch.BatchKeyAdapter;
import com.hazeltask.batch.Bundler;
import com.hazeltask.core.metrics.MetricNamer;
import com.hazeltask.core.metrics.ScopeFirstMetricNamer;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorService;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorServiceBuilder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * Many times its more efficient to do work if they are grouped into a small
 * batch. for example, if you need to perform a lot of SELECT queries its more
 * efficient to SELECT 100 items 1 time than it is to SELECT 1 item 100 times.
 * 
 * This abstraction to the Partitioned Executor Service will help you do this
 * bundling.
 * 
 * By default it will queue up items into a MultiMap backed by hazelcast.  This 
 * will prevent you from submitting the same item more than once... before its
 * flushed, bundled, and submitted for execution.  If you want to prevent submitting
 * the same item up to the point of its execution, use the withDuplicatePrevention
 * option.
 * 
 * If add() operations are too slow, you can opt to use withLocalBufferingOnly which
 * will use a localMultiMap instead of a hazelcast backed version.  This will be much
 * faster, but you risk losing items if the node goes down.
 * 
 * Note: If you are using bundling, do not add Runnables/Callables to the 
 * DistributedExecutorService by hand.
 * 
 * TODO: implement a configuration verification process... send a message out to other nodes,
 * get their configurations.  Compare with this one.... log incompatibilities.  For example:
 * if 1 node uses preventDuplicates, all nodes must use it... or it won't work.
 * 
 * @author jclawson
 * 
 */
public class DeferredWorkBundlerBuilder {
    
    public static AtomicInteger atomicServiceIndex = new AtomicInteger(1);
    
    public static <I> InternalBuilderStep1<I> builder(String topologyName){
        return builder(Hazelcast.getDefaultInstance(), topologyName);
    }
    
    public static <I> InternalBuilderStep1<I> builder(HazelcastInstance hazelcast, String topologyName){
        return new InternalBuilderStep1<I>(hazelcast, topologyName);
    }
    
    public static class InternalBuilderStep1<I> {
        protected final HazelcastWorkTopology topology;

        public InternalBuilderStep1(HazelcastInstance hazelcast, String topologyName) {
            topology = HazelcastWorkTopology.getOrCreate(topologyName, hazelcast);
        }

        /**
         * We must be able to, given a piece of work, identify its group
         * 
         * @param adapter
         * @return
         */
        public InternalBuilderStep2<I> withIdentifier(BatchKeyAdapter<I> partitioner) {
            return new InternalBuilderStep2<I>(this, partitioner);
        }
    }

    public static class InternalBuilderStep2<I> {
        private BatchKeyAdapter<I>       partitioner;
        protected InternalBuilderStep1<I> step1;
        
        public InternalBuilderStep2(InternalBuilderStep1<I> step1, BatchKeyAdapter<I> partitioner) {
            this.partitioner = partitioner;
            this.step1 = step1;
        }

        public InternalBuilderStep3<I> withBundler(Bundler<I> bundler) {
            return new InternalBuilderStep3<I>(this, partitioner, bundler);
        }

    }

    public static class InternalBuilderStep3<I> {
        protected HazelcastWorkTopology topology;
        protected InternalBuilderStep2<I> step2;
        protected BatchKeyAdapter<I>       partitioner;
        protected Bundler<I>        bundler;
        protected int               flushSize;
        protected int               minBundleSize;
        protected int               maxBundleSize;
        protected long              flushTTL;
        protected boolean           localBuffering;
        protected boolean           preventDuplicates;
        protected long              maxDuplicatePreventionTTL;
        protected boolean           doWork;
        protected int 				threadCount;
        protected MetricNamer 		metricNamer;
	    protected MetricsRegistry 	metricsRegistry;

        public InternalBuilderStep3(InternalBuilderStep2<I> step2, BatchKeyAdapter<I> partitioner, Bundler<I> bundler) {
            this.step2              = step2;
            this.partitioner        = partitioner;
            this.bundler            = bundler;

            // defaults ------
            this.flushSize          = 200;
            this.minBundleSize      = 50;
            this.maxBundleSize      = 100;
            this.flushTTL           = 10000;
            this.localBuffering     = false;
            this.preventDuplicates  = false;
            this.maxDuplicatePreventionTTL = 1800000; // 30 Minutes
            this.topology = step2.step1.topology;
            this.doWork = true;
            this.threadCount 		= 4;
        }

        public InternalBuilderStep3<I> withFlushSize(int flushSize) {
            this.flushSize = flushSize;
            return this;
        }

        public InternalBuilderStep3<I> withMinBundleSize(int minBundleSize) {
            this.minBundleSize = minBundleSize;
            return this;
        }
        
        public InternalBuilderStep3<I> withMaxBundleSize(int maxBundleSize) {
            this.maxBundleSize = maxBundleSize;
            return this;
        }

        public InternalBuilderStep3<I> withFlushTTL(long flushTTL) {
            this.flushTTL = flushTTL;
            return this;
        }
        
        public  InternalBuilderStep3<I> doNotDoWork() {
            this.doWork = false;
            return this;
        }
        
        /**
         * By default we do clustered buffering in hazelcast to prevent losing work
         * If you would rather things go faster, and take the chance of losing work
         * if nodes go down, then use local buffering
         * 
         * local buffering is WAY faster
         * 
         * @return
         */
        public InternalBuilderStep3<I> withLocalBufferingOnly() {
            this.localBuffering = true;
            return this;
        }
        
        /**
         * With this option, we will add items to a hazelcast map and prevent adding
         * items that already exist in this map.  Items will be removed from the map
         * when the bundled work is started.  This doesn't protect it completely,
         * but it does help if the system continuously submits duplicate items.
         * 
         * Using this will reduce the performance of adding items but may increase the
         * performance of the work system as a whole depending on how often duplicate 
         * items are added.
         * 
         * 
         * @return
         */
        public InternalBuilderStep3<I> withDuplicatePrevention() {
            this.preventDuplicates = true;
            return this;
        }
        
        /**
         * If you are using withDuplicatePrevention, this TTL will mark the maximum
         * amount of time an item is allowed to stay in the duplicate prevention map
         * before being automatically removed.  This is mostly a protection in case 
         * something goes wrong.
         * 
         * defaults to 30 minutes
         * 
         * @param time - a value of 0 means infinite
         * @return
         */
        public InternalBuilderStep3<I> withMaxDuplicatePreventionTTL(long time){
            this.maxDuplicatePreventionTTL = time;
            return this;
        }
        
        public InternalBuilderStep3<I> withThreadCount(int numberOfThreads) {
	        this.threadCount = numberOfThreads;
	        return this;
	    }

        /**
	     * Enables statistics with Yammer Metrics.  Make sure you include the yammer metrics
	     * optional library!
	     * 
	     * @param namer - Helper for naming metrics according to custom specifications
	     * @return
	     */
	    public InternalBuilderStep3<I> enableStatisics(MetricNamer namer, MetricsRegistry metrics) {
	        this.metricNamer = namer;
	        this.metricsRegistry = metrics;
	        return this;
	    }
	    
	    public InternalBuilderStep3<I> enableStatisics(MetricsRegistry metrics) {
	        this.metricNamer = new ScopeFirstMetricNamer();
	        this.metricsRegistry = metrics;
	        return this;
	    }
	    
	    public InternalBuilderStep3<I> enableStatisics() {
	        this.metricNamer = new ScopeFirstMetricNamer();
	        this.metricsRegistry = Metrics.defaultRegistry();
	        return this;
	    }
        
        public DeferredWorkBundler<I> build() {
            
            //build our distributed executor service
            DistributedExecutorService svc = DistributedExecutorServiceBuilder.builder(topology.getHazelcast(), topology.getName())
                .withWorkKeyAdapter(step2.partitioner)
                .withDisabledWorkers(!this.doWork)
                .withThreadCount(threadCount)
                .enableStatisics(metricNamer, metricsRegistry)
                .build();
            
        	return new DeferredWorkBundler<I>(this, svc, metricNamer, metricsRegistry);
        }
    }
}
