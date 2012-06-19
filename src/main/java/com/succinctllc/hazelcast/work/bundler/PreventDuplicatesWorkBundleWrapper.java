package com.succinctllc.hazelcast.work.bundler;

import java.util.Collection;
import java.util.logging.Level;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.succinctllc.core.concurrent.collections.grouped.Groupable;
import com.succinctllc.hazelcast.work.HazelcastWorkManager;
import com.succinctllc.hazelcast.work.WorkId;

/**
 * The purpose of this wrapper is to remove the items from the 
 * global item map that prevents duplication.  It must do this 
 * prior to executing the actual work so that we ensure the items
 * will always be removed from the map.
 * 
 * TODO: lets replace the coupling with the HazelcastWorkManager with
 * a Google Guava Event Bus instead.  Then, we can have event listeners
 * deal with the issue and optionally make the bus async.
 * 
 * @author jclawson
 *
 */
public class PreventDuplicatesWorkBundleWrapper<I> implements WorkBundle<I> {
    private static final long serialVersionUID = 1L;
    private static ILogger LOGGER = Logger.getLogger(PreventDuplicatesWorkBundleWrapper.class.getName());
    
    
    private final WorkId workRef;
    private final String topology;
    
    private final WorkBundle<I> delegate;
    protected PreventDuplicatesWorkBundleWrapper(String topology, WorkBundle<I> delegate) {
        this.delegate = delegate;
        workRef = delegate.getWorkId();
        this.topology = topology;
    }
    
    public void run() {
        try {
            HazelcastWorkManager.<I>getDeferredWorkBundler(topology)
                .removePreventDuplicateItems(this);
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "An error occurred while removing items from duplicate prevention map", e);
        }
        this.delegate.run();
    }

    public WorkId getWorkId() {
        return workRef;
    }

    public Collection<I> getItems() {
        return delegate.getItems();
    }

	public String getGroup() {
		return workRef.getGroup();
	}

	public String getUniqueIdentifier() {
		return workRef.getId();
	}

}
