package com.hazeltask.batch;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.hazeltask.config.BundlerConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.hazelcast.MultiMapProxy;

public class HazelcastBatchClusterService<I> implements IBatchClusterService<I> {

    private final BundlerConfig<I> config;
    private final MultiMapProxy<String, I> multimap;
    private final String topologyName;
    private final BatchKeyAdapter<I> keyer;
    
    public HazelcastBatchClusterService(HazeltaskConfig htConfig, BundlerConfig<I> config) {
        this.config = config;
        this.topologyName = htConfig.getTopologyName();
        this.keyer = config.getBatchKeyAdapter();
        
        if(config.isLocalBuffering()) {
            multimap = MultiMapProxy.localMultiMap(HashMultimap.<String, I>create());
        } else {
            multimap = MultiMapProxy.clusteredMultiMap(
                        htConfig.getHazelcast()
                            .<String, I>getMultiMap(name("batch-items"))
                    );
        }
        
    }
    
    private String name(String name) {
        return topologyName + "-" + name;
    }
    
    public boolean addToBatch(I item) {
        return multimap.put(keyer.getItemGroup(item), item);
    }
    
  public Map<String, Integer> getNonZeroLocalGroupSizes() {
      Set<String> localKeys = multimap.localKeySet();   
      Map<String, Integer> result =  new HashMap<String, Integer>(localKeys.size());
      for(String key : localKeys) {
          int size = multimap.get(key).size();
          if(size > 0)
              result.put(key, size);
      }
      return result;
}
    
    public List<I> getItems(String group) {
        return multimap.getAsList(group);
    }

    public int removeItems(String group, Collection<I> items) {
        return multimap.removeAll(group, (Collection<Object>) items);
    }

    public boolean addToPreventDuplicateSet(String itemId) {
       throw new RuntimeException("Not implemented yet");
    }

    public boolean isInPreventDuplicateSet(String itemId) {
        throw new RuntimeException("Not implemented yet");
    }

    public boolean removePreventDuplicateItem(String itemId) {
        throw new RuntimeException("Not implemented yet");
    }


}
