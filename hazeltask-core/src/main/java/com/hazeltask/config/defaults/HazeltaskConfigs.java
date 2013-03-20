package com.hazeltask.config.defaults;

import java.io.Serializable;

import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.config.HazeltaskConfig;

public class HazeltaskConfigs {
    public static HazeltaskSimpleConfig basic() {
        return new HazeltaskSimpleConfig();
    }
    
    public static <GROUP extends Serializable> HazeltaskGroupableConfig<GROUP> groupable() {
        return new HazeltaskGroupableConfig<GROUP>();
    }
    
    public static <GROUP extends Serializable> HazeltaskConfig<GROUP> advanced() {
        return new HazeltaskConfig<GROUP>();
    }
    
    public static class HazeltaskSimpleConfig extends HazeltaskConfig<Integer> {
        //TODO: wrap executor config methods to ensure proper configuration
    }
    
    public static class HazeltaskGroupableConfig<GROUP extends Serializable> extends HazeltaskConfig<GROUP> {
        //TODO: wrap executor config methods to ensure proper configuration
        public HazeltaskGroupableConfig() {
            ExecutorConfig<GROUP> config = ExecutorConfigs.basicGroupable();
            super.withExecutorConfig(config);
        }
    }
}
