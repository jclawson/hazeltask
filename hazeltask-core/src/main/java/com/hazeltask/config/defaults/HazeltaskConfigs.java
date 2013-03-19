package com.hazeltask.config.defaults;

import java.io.Serializable;
import java.util.UUID;

import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.config.HazeltaskConfig;

public class HazeltaskConfigs {
    public static HazeltaskSimpleConfig basic() {
        return new HazeltaskSimpleConfig();
    }
    
    public static <GROUP extends Serializable> HazeltaskGroupableConfig<GROUP> groupable() {
        return new HazeltaskGroupableConfig<GROUP>();
    }
    
    public static <ID extends Serializable, GROUP extends Serializable> HazeltaskConfig<ID, GROUP> advanced() {
        return new HazeltaskConfig<ID, GROUP>();
    }
    
    public static class HazeltaskSimpleConfig extends HazeltaskConfig<UUID, Integer> {
        //TODO: wrap executor config methods to ensure proper configuration
    }
    
    public static class HazeltaskGroupableConfig<GROUP extends Serializable> extends HazeltaskConfig<UUID, GROUP> {
        //TODO: wrap executor config methods to ensure proper configuration
        public HazeltaskGroupableConfig() {
            ExecutorConfig<UUID, GROUP> config = ExecutorConfigs.basicGroupable();
            super.withExecutorConfig(config);
        }
    }
}
