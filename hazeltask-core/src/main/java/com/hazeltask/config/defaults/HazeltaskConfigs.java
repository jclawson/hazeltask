package com.hazeltask.config.defaults;

import java.io.Serializable;

import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.config.HazeltaskConfig;

public class HazeltaskConfigs {
    /**
     * If you just need a DistributedExecutor service that will do failover recovery and 
     * task rebalancing, use this option.
     * @return
     */
    public static HazeltaskSimpleConfig basic() {
        return new HazeltaskSimpleConfig();
    }
    
    /**
     * If you want to group your tasks into a specific group and all your tasks implement 
     * Groupable, then use this option
     * 
     * @return
     */
    public static <GROUP extends Serializable> HazeltaskGroupableConfig<GROUP> groupable() {
        return new HazeltaskGroupableConfig<GROUP>();
    }
    
    /**
     * If your tasks don't implement Groupable and you need to specifiy your own 
     * TaskGroupAdapter and other advanced options, use this configuration as a 
     * starting point.
     * 
     * @return
     */
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
