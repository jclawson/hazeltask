package com.hazeltask.batch;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.hazeltask.config.BundlerConfig;
import com.hazeltask.executor.DistributedExecutorService;

public class BatchingContext<I> {
    private final IBatchFactory<I, ?, Serializable>             bundler;
    private final BatchKeyAdapter<I, ?,?,?,?>     batchKeyAdapter;
    private final SetMultimap<Serializable, I> multimap;
    private final BundlerConfig<I, ?, ?, ?> batchingConfig;
    private final DistributedExecutorService svc;

    @SuppressWarnings("unchecked")
    protected BatchingContext(DistributedExecutorService svc, BundlerConfig<I,?, ?, ?> batchingConfig) {
        this.batchingConfig = batchingConfig;
        
        //TODO: fix this cast to something better.  I want to hide the parameterization from the developer
        this.bundler = (IBatchFactory<I, ?, Serializable>) batchingConfig.getBundler();
        this.batchKeyAdapter = batchingConfig.getBatchKeyAdapter();
        this.multimap = HashMultimap.<Serializable, I> create();
        this.svc = svc;
    }
    /**
     * 
     * @param item
     * @return true if added, false if already exists
     */
    public boolean addItem(I item) {
        Serializable group = batchKeyAdapter.getItemGroup(item);
        boolean result = multimap.put(group, item);
        if(result) {
            checkAndSubmit(group);
        }
        return result;
    }

    public void submit() {
        for(Serializable group : new HashSet<Serializable>(multimap.keySet())) {
            submit(group);
        }
    }
    
    private void checkAndSubmit(Serializable group) {
        if(multimap.get(group).size() >= batchingConfig.getFlushSize()) {
            submit(group);
        }
    }
    
    private void submit(Serializable group) {
        Set<I> items = multimap.removeAll(group);
        if(items.size() > 0) {
            TaskBatch<I, ?, ?> task = bundler.createBatch(group, items);
            svc.execute(task);
        }
    }
    
    
}
