package com.hazeltask;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.hazeltask.config.HazeltaskConfig;

public final class Hazeltask {
    public static final String DEFAULT_TOPOLOGY = "DefaultTopology";
    public static ConcurrentMap<String, HazeltaskInstance> instances = new ConcurrentHashMap<String, HazeltaskInstance>();

    private Hazeltask() {

    }

    public static HazeltaskInstance getHazeltaskInstanceByTopology(String topology) {
        return instances.get(topology);
    }
    
    /**
     * @deprecated This is deprecated because it relies in the deprecated Hazelcast.getDefaultInstance
     * @see newHazeltaskInstance
     * @see 
     * @return
     */
    @Deprecated
    public static HazeltaskInstance getDefaultInstance() {
        HazeltaskConfig hazeltaskConfig = new HazeltaskConfig();
        HazeltaskInstance instance = getHazeltaskInstanceByTopology(DEFAULT_TOPOLOGY);
        if(instance == null) {
            try {
                return newHazeltaskInstance(hazeltaskConfig);
            } catch (IllegalStateException e) {
                instance = getHazeltaskInstanceByTopology(DEFAULT_TOPOLOGY);
            }
        }
        
        if(instance == null) {
            throw new RuntimeException("Unable to construct default instance");
        }
        
        return instance;
    }
    
    public static HazeltaskInstance newHazeltaskInstance(HazeltaskConfig hazeltaskConfig) {
        HazeltaskInstance instance = new HazeltaskInstance(hazeltaskConfig);
        HazeltaskTopology topology = instance.getTopology();
        if(instances.putIfAbsent(topology.getName(), instance) != null) {
            throw new IllegalStateException("An instance for the topology "+topology+" already exists!");
        }
        return instance;
    }

}
