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
public class HazelcastBatchClusterService<I, ITEM_ID extends Serializable, GROUP extends Serializable> implements IBatchClusterService<I,ITEM_ID,GROUP> {

    private final MultiMapProxy<GROUP, I> multimap;
    private final String topologyName;
    private final BatchKeyAdapter<I,?,ITEM_ID,?,GROUP> keyer;
    
    public HazelcastBatchClusterService(HazeltaskConfig htConfig) {
        this.topologyName = htConfig.getTopologyName();
        BundlerConfig<I,ITEM_ID,?,GROUP> config = htConfig.getBundlerConfig();
        this.keyer = config.getBatchKeyAdapter();
        
        if(config.isLocalBuffering()) {
            multimap = MultiMapProxy.localMultiMap(HashMultimap.<GROUP, I>create());
        } else {
            multimap = MultiMapProxy.clusteredMultiMap(
                        htConfig.getHazelcast()
                            .<GROUP, I>getMultiMap(name("batch-items"))
                    );
        }
        
    }
    
    private String name(String name) {
        return topologyName + "-" + name;
    }
    
    public boolean addToBatch(I item) {
        return multimap.put(keyer.getItemGroup(item), item);
    }
    
  public Map<GROUP, Integer> getNonZeroLocalGroupSizes() {
      Set<GROUP> localKeys = multimap.localKeySet();   
      Map<GROUP, Integer> result =  new HashMap<GROUP, Integer>(localKeys.size());
      for(GROUP key : localKeys) {
          int size = multimap.get(key).size();
          if(size > 0)
              result.put(key, size);
      }
      return result;
}
    
    public List<I> getItems(GROUP group) {
        return multimap.getAsList(group);
    }

    @SuppressWarnings("unchecked")
    public int removeItems(GROUP group, Collection<I> items) {
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
