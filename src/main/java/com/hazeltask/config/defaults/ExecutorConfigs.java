package com.hazeltask.config.defaults;

import java.io.Serializable;
import java.util.UUID;

import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.executor.task.DefaultGroupableTaskIdAdapter;
import com.hazeltask.executor.task.DefaultTaskIdAdapter;

/**
 * This class contains a lot of default configurations for easy and quick setup for
 * common use cases
 * 
 * @author jclawson
 *
 */
public class ExecutorConfigs {
    /**
     * You must specify your task id adapter so the system knows how to get those values given a task
     * @return
     */
    public  static <ID extends Serializable, GROUP extends Serializable> ExecutorConfig<ID,GROUP> advanced() {
        return new ExecutorConfig<ID,GROUP>()
                    .withTaskIdAdapter(null);
    }
    
    /**
     * This configuration doesn't require any further configuration to work.
     * This is a good replacement for the default hazelcast executor service which you can 
     * tune a little for your own usage
     * 
     * @return
     */
    public static ExecutorConfig<String,String> basic() {
        return new ExecutorConfig<String,String>()
                    .withTaskIdAdapter(new DefaultTaskIdAdapter());
    }
    
    /**
     * This configuration requires that all your tasks you submit to the system implement
     * the Groupable interface.  By default, it will round robin tasks from each group
     * 
     * Tasks will be tracked internally in the system by randomly generated UUIDs
     * 
     * @return
     */
    public static <GROUP extends Serializable> ExecutorConfig<UUID, GROUP> basicGroupable() {
        return new ExecutorConfig<UUID,GROUP>()
                    .withTaskIdAdapter(new DefaultGroupableTaskIdAdapter<GROUP>());
    }
}
