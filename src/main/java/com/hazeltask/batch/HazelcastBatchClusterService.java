package com.hazeltask.batch;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.hazeltask.config.BundlerConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.hazelcast.MultiMapProxy;

//TODO: add better generics...
public class HazelcastBatchClusterService<I> implements IBatchClusterService<I> {

    private final MultiMapProxy<Serializable, I> multimap;
    private final String topologyName;
    private final BatchKeyAdapter<I,?,?,?,?> keyer;
    
    public HazelcastBatchClusterService(HazeltaskConfig htConfig) {
        this.topologyName = htConfig.getTopologyName();
        BundlerConfig<I,?,?,?> config = htConfig.getBundlerConfig();
        this.keyer = config.getBatchKeyAdapter();
        
        if(config.isLocalBuffering()) {
            multimap = MultiMapProxy.localMultiMap(HashMultimap.<Serializable, I>create());
        } else {
            multimap = MultiMapProxy.clusteredMultiMap(
                        htConfig.getHazelcast()
                            .<Serializable, I>getMultiMap(name("batch-items"))
                    );
        }
        
    }
    
    private String name(String name) {
        return topologyName + "-" + name;
    }
    
    public boolean addToBatch(I item) {
        return multimap.put(keyer.getItemGroup(item), item);
    }
    
  public Map<Serializable, Integer> getNonZeroLocalGroupSizes() {
      Set<Serializable> localKeys = multimap.localKeySet();   
      Map<Serializable, Integer> result =  new HashMap<Serializable, Integer>(localKeys.size());
      for(Serializable key : localKeys) {
          int size = multimap.get(key).size();
          if(size > 0)
              result.put(key, size);
      }
      return result;
}
    
    public List<I> getItems(Serializable group) {
        return multimap.getAsList(group);
    }

    @SuppressWarnings("unchecked")
    public int removeItems(Serializable group, Collection<I> items) {
        return multimap.removeAll(group, (Collection<Object>) items);
    }

    public boolean addToPreventDuplicateSet(Serializable itemId) {
       throw new RuntimeException("Not implemented yet");
    }

    public boolean isInPreventDuplicateSet(Serializable itemId) {
        throw new RuntimeException("Not implemented yet");
    }

    public boolean removePreventDuplicateItem(Serializable itemId) {
        throw new RuntimeException("Not implemented yet");
    }


}
