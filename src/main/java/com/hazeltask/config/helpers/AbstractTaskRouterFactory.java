package com.hazeltask.config.helpers;

import java.io.Serializable;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import com.hazeltask.core.concurrent.collections.router.ListRouter;
import com.hazeltask.core.concurrent.collections.router.ListRouterFactory;
import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;
import com.hazeltask.executor.task.HazeltaskTask;

/**
 * TODO: figure out a better way to hide the internals of this API.  I don't want to exposed HazeltaskTask to 
 * the developer;
 * 
 * TODO: can we get rid of the callable API?
 * 
 * @author jclawson
 *
 * @param <ID>
 * @param <GROUP>
 */
public abstract class AbstractTaskRouterFactory<ID extends Serializable, GROUP extends Serializable> implements ListRouterFactory<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID,GROUP>>>> {

    public abstract ListRouter<Entry<GROUP, ITrackedQueue<?>>> createTaskRouter(Callable<List<Entry<GROUP, ITrackedQueue<?>>>> listAcessor);
    
    @Override
    public final ListRouter<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID, GROUP>>>> createRouter(
            final List<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID, GROUP>>>> list) {
       return createRouter(new Callable<List<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID, GROUP>>>>>(){
        @Override
        public List<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID, GROUP>>>> call() throws Exception {
            return list;
        }   
       });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public final ListRouter<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID, GROUP>>>> createRouter(
            Callable<List<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID, GROUP>>>>> list) {
            return (ListRouter<Entry<GROUP, ITrackedQueue<HazeltaskTask<ID, GROUP>>>>) ((ListRouter) createTaskRouter((Callable)list));
    }

}
