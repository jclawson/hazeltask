package com.succinctllc.hazelcast.work;

import com.hazelcast.core.ManagedContext;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorServiceAware;

public class HazelcastWorkManagedContext implements ManagedContext {

	private ManagedContext delegate;
	private HazelcastWorkManagedContext(ManagedContext delegate) {
		this.delegate = delegate;
	}
	
	public void initialize(Object obj) {
		delegate.initialize(obj);
		
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
	
	public static synchronized ManagedContext wrap(ManagedContext context) {
		if(context instanceof HazelcastWorkManagedContext)
			return context;
		
		//final test to see if our managed context appears in some chain
		TestContext ctxTest = new TestContext();
		context.initialize(ctxTest);
		
		if(ctxTest.isWrapped)
			return context;
			
		return new HazelcastWorkManagedContext(context);
	}
	
	private static class TestContext {
		boolean isWrapped = false;		
	}
	
}
