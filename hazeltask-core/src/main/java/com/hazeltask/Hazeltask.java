package com.hazeltask;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.hazeltask.config.HazeltaskConfig;

public final class Hazeltask {
    public static final String DEFAULT_TOPOLOGY = "DefaultTopology";
    public static ConcurrentMap<String, HazeltaskInstance<?,?>> instances = new ConcurrentHashMap<String, HazeltaskInstance<?,?>>();

    private Hazeltask() {

    }

    @SuppressWarnings("unchecked")
    public static <ID extends Serializable, GROUP extends Serializable> HazeltaskInstance<ID,GROUP> getHazeltaskInstanceByTopology(String topology) {
        return (HazeltaskInstance<ID,GROUP>) instances.get(topology);
    }
    
    /**
     * @deprecated This is deprecated because it relies in the deprecated Hazelcast.getDefaultInstance
     * @see newHazeltaskInstance
     * @see 
     * @return
     */
    @Deprecated
    public static HazeltaskInstance<UUID, Integer> getDefaultInstance() {
        HazeltaskConfig<UUID, Integer> hazeltaskConfig = new HazeltaskConfig<UUID, Integer>();
        HazeltaskInstance<UUID, Integer> instance = getHazeltaskInstanceByTopology(DEFAULT_TOPOLOGY);
        if(instance == null) {
            try {
                return (HazeltaskInstance<UUID, Integer>) newHazeltaskInstance(hazeltaskConfig);
            } catch (IllegalStateException e) {
                instance = getHazeltaskInstanceByTopology(DEFAULT_TOPOLOGY);
            }
        }
        
        if(instance == null) {
            throw new RuntimeException("Unable to construct default instance");
        }
        
        return instance;
    }
    
    public static <ID extends Serializable, GROUP extends Serializable> HazeltaskInstance<ID,GROUP> newHazeltaskInstance(HazeltaskConfig<ID,GROUP> hazeltaskConfig) {
        HazeltaskInstance<ID,GROUP> instance = new HazeltaskInstance<ID,GROUP>(hazeltaskConfig);
        HazeltaskTopology<ID, GROUP> topology = instance.getTopology();
        if(instances.putIfAbsent(topology.getName(), instance) != null) {
            throw new IllegalStateException("An instance for the topology "+topology+" already exists!");
        }
        instance.start();
        return instance;
    }

}
