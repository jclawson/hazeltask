package com.hazeltask.batch;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.hazeltask.config.BundlerConfig;
import com.hazeltask.executor.DistributedExecutorService;

public class BatchingContext<I> {
    private final IBatchFactory<I>             bundler;
    private final BatchKeyAdapter<I>     batchKeyAdapter;
    private final SetMultimap<String, I> multimap;
    private final BundlerConfig<I> batchingConfig;
    private final DistributedExecutorService svc;

    protected BatchingContext(DistributedExecutorService svc, BundlerConfig<I> batchingConfig) {
        this.batchingConfig = batchingConfig;
        this.bundler = batchingConfig.getBundler();
        this.batchKeyAdapter = batchingConfig.getBatchKeyAdapter();
        this.multimap = HashMultimap.<String, I> create();
        this.svc = svc;
    }
    /**
     * 
     * @param item
     * @return true if added, false if already exists
     */
    public boolean addItem(I item) {
        String group = batchKeyAdapter.getItemGroup(item);
        boolean result = multimap.put(group, item);
        if(result) {
            checkAndSubmit(group);
        }
        return result;
    }

    public void submit() {
        for(String group : new HashSet<String>(multimap.keySet())) {
            submit(group);
        }
    }
    
    private void checkAndSubmit(String group) {
        if(multimap.get(group).size() >= batchingConfig.getFlushSize()) {
            submit(group);
        }
    }
    
    private void submit(String group) {
        Set<I> items = multimap.removeAll(group);
        if(items.size() > 0) {
            TaskBatch<I> task = bundler.createBatch(group, items);
            svc.execute(task);
        }
    }
    
    
}
