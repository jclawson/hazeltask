package com.hazeltask;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.hazeltask.config.HazeltaskConfig;

public final class Hazeltask {
    public static final String DEFAULT_TOPOLOGY = "DefaultTopology";
    public static ConcurrentMap<String, HazeltaskInstance<?>> instances = new ConcurrentHashMap<String, HazeltaskInstance<?>>();

    private Hazeltask() {

    }

    @SuppressWarnings("unchecked")
    public static <GROUP extends Serializable> HazeltaskInstance<GROUP> getInstanceByName(String topology) {
        return (HazeltaskInstance<GROUP>) instances.get(topology);
    }
    
    /**
     * @deprecated This is deprecated because it relies in the deprecated Hazelcast.getDefaultInstance
     * @see newHazeltaskInstance
     * @see 
     * @return
     */
    @Deprecated
    public static HazeltaskInstance<Integer> getDefaultInstance() {
        HazeltaskConfig<Integer> hazeltaskConfig = new HazeltaskConfig<Integer>();
        HazeltaskInstance<Integer> instance = getInstanceByName(DEFAULT_TOPOLOGY);
        if(instance == null) {
            try {
                return (HazeltaskInstance<Integer>) newHazeltaskInstance(hazeltaskConfig);
            } catch (IllegalStateException e) {
                instance = getInstanceByName(DEFAULT_TOPOLOGY);
            }
        }
        
        if(instance == null) {
            throw new RuntimeException("Unable to construct default instance");
        }
        
        return instance;
    }
    
    public static <GROUP extends Serializable> HazeltaskInstance<GROUP> newHazeltaskInstance(HazeltaskConfig<GROUP> hazeltaskConfig) {
        HazeltaskInstance<GROUP> instance = new HazeltaskInstance<GROUP>(hazeltaskConfig);
        HazeltaskTopology<GROUP> topology = instance.getTopology();
        if(instances.putIfAbsent(topology.getName(), instance) != null) {
            throw new IllegalStateException("An instance for the topology "+topology+" already exists!");
        }
        instance.start();
        return instance;
    }

}
