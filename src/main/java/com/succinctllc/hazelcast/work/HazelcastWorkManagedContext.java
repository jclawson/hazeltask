package com.succinctllc.hazelcast.work;

import java.lang.reflect.Field;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.FactoryImpl.HazelcastInstanceProxy;
import com.succinctllc.hazelcast.util.MemberTasks.MemberResponseCallable;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorServiceAware;

public class HazelcastWorkManagedContext implements ManagedContext {

	private ManagedContext delegate;
	private HazelcastWorkManagedContext(ManagedContext delegate) {
		this.delegate = delegate;
	}
	
	public void initialize(Object obj) {
		delegate.initialize(obj);
		
		if(obj instanceof MemberResponseCallable) {
		    obj = ((MemberResponseCallable<?>) obj).getDelegate();
		}
		
		if(obj instanceof DistributedExecutorServiceAware) {
			DistributedExecutorServiceAware aware = (DistributedExecutorServiceAware) obj;
			aware.setDistributedExecutorService(
				HazelcastWorkManager.getDistributedExecutorService(aware.getTopology())
			);
		}
		
		//TODO: add support for more "aware" objects
		
		//setup test context for testing if the managed context is already wrapped
		if(obj instanceof TestContext) {
			((TestContext) obj).isWrapped = true;
		}
	}
	
	/**
	 * This method is a little hacky.  It uses reflection to wrap the existing
	 * ManagedContext in hazelcast in order to provide support for our custom
	 * ManagedContext
	 * 
	 * @param hazelcast
	 */
	public static synchronized void apply(HazelcastInstance hazelcast) {	
	    FactoryImpl factory;
	    if(hazelcast instanceof HazelcastInstanceProxy) {
	        factory = ((HazelcastInstanceProxy) hazelcast).getFactory();
	    } else {
	        factory = (FactoryImpl)hazelcast;
	    }
	    ManagedContext context;
        try {
            Field f = FactoryImpl.class.getDeclaredField("managedContext");
            f.setAccessible(true);
            context = (ManagedContext) f.get(factory);
	    
    		//final test to see if our managed context appears in some chain
    		TestContext ctxTest = new TestContext();
    		context.initialize(ctxTest);
    		
    		if(!ctxTest.isWrapped) {
    			f.set(factory, new HazelcastWorkManagedContext(context));
    		}
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
	}
	
	private static class TestContext {
		boolean isWrapped = false;		
	}
	
}
