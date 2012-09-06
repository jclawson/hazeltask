package com.hazeltask.batch;

import java.util.Collection;
import java.util.Map;

import com.hazeltask.config.HazeltaskConfig;

public class HazelcastBatchClusterService<I> implements IBatchClusterService<I> {

    public HazelcastBatchClusterService(HazeltaskConfig config) {
        
    }
    
    public boolean addToPreventDuplicateSet(String itemId) {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean removePreventDuplicateItem(String itemId) {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean addToBatch(I item) {
        // TODO Auto-generated method stub
        return false;
    }

    public Map<String, Integer> getGroupSizes() {
        // TODO Auto-generated method stub
        return null;
    }

    public Collection<I> drain(String group) {
        // TODO Auto-generated method stub
        return null;
    }

}
