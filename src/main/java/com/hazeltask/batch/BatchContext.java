package com.hazeltask.batch;

import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.hazeltask.config.BundlerConfig;
import com.hazeltask.config.HazeltaskConfig;

public class BatchContext<I> {
    private final Bundler<I>             bundler;
    private final BatchKeyAdapter<I>     batchKeyAdapter;
    private final SetMultimap<String, I> multimap;
    private final BundlerConfig<I> batchingConfig;
    private final HazeltaskConfig hazeltaskConfig;

    protected BatchContext(TaskBatchingService<I> svc, HazeltaskConfig hazeltaskConfig, BundlerConfig<I> batchingConfig) {
        this.batchingConfig = batchingConfig;
        this.bundler = batchingConfig.getBundler();
        this.batchKeyAdapter = batchingConfig.getBatchKeyAdapter();
        this.multimap = HashMultimap.<String, I> create();
        this.hazeltaskConfig = hazeltaskConfig;
    }
    /**
     * 
     * @param item
     * @return true if added, false if already exists
     */
    public boolean addItem(I item) {
        String group = batchKeyAdapter.getItemGroup(item);
        //TODO: submit batches as we go instead of waiting until the end
        boolean result = multimap.put(group, item);
        if(result) {
            checkAndSubmit(group);
        }
        return result;
    }

    public void submit() {
        
    }
    
    private void submit(String group) {
        Set<I> items = multimap.get(group);
        if(items.size() > 0) {
            WorkBundle<I> task = bundler.bundle(group, items);
            if(this.batchingConfig.isPreventDuplicates()) {
                //if we are preventing duplicates, then we need to wrap it
                task = new PreventDuplicatesWorkBundleWrapper<I>(hazeltaskConfig.getTopologyName(), task);
            }
        }
    }
    
    private void checkAndSubmit(String group) {
        if(multimap.get(group).size() >= batchingConfig.getFlushSize()) {
            submit(group);
        }
    }
}
