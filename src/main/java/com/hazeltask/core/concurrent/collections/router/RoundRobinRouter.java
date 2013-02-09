package com.hazeltask.core.concurrent.collections.router;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * This round robin router is concurrency safe.  It is not guaranteed to be
 * perfectly accurate if the list is changing out from underneath it.  It will 
 * never throw an exception.  It will only return null if the list is known to be
 * empty.
 * 
 * If an unexpected condition occurs(the list gets smaller such that it would try to 
 * select an index that no longer exists), it will retry its execution 
 * keeping track of the number of retries.  It will retry a max of 100 times
 * 
 * @author jclawson
 *
 * @param <T>
 */
public class RoundRobinRouter<T> implements ListRouter<T> {
    
    private static ListRouterFactory<Object> FACTORY = new ListRouterFactory<Object>() {        
        public ListRouter<Object> createRouter(Callable<List<Object>> list) {
            return new RoundRobinRouter<Object>(list);
        }
        
        public ListRouter<Object> createRouter(List<Object> list) {
            return new RoundRobinRouter<Object>(list);
        }
    };
    
    @SuppressWarnings("unchecked")
    public static <E> ListRouterFactory<E> newFactory() {
        return (ListRouterFactory<E>) FACTORY;
    }
    
    private List<T> list;
    private Callable<List<T>> fetchList;
    private static final int MAX_TRIES = 100;
    ILogger logger = Logger.getLogger(RoundRobinRouter.class.getName());
    
    private AtomicInteger lastIndex = new AtomicInteger(-1);
    
    public RoundRobinRouter(List<T> list){
        this.list = list;
    }
    
    public RoundRobinRouter(Callable<List<T>> fetchList){
        this.fetchList = fetchList;
    }
    
    public T next(){
        return next(1, 0);
    }
    
    public T next(int tries, int numSkipped) {
        List<T> list = getList();
        int size = list.size();
        
        if(numSkipped >= size) {
            return null;
        }
        
        if(tries >= MAX_TRIES) {
            logger.log(Level.WARNING, "RoundRobin Router exceeded MAX_TRIES while attempting to get the next item");
            return null;
        }
        
        int index = lastIndex.incrementAndGet() % size;
        
        try {             
            T result = list.get(index);
            /*if(skipper != null && skipper.shouldSkip(result)) {
                return next(1, numSkipped+1);
            } else {
                return result;
            }*/
            return result;
        } catch(IndexOutOfBoundsException e) {
            //list changed under us... try again          
            return next(tries+1, numSkipped);
        }
    }
    
    private List<T> getList(){
        try {
            return (list == null) ? fetchList.call() : list;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
