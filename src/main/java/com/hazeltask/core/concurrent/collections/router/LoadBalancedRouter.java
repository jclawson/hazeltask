package com.hazeltask.core.concurrent.collections.router;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.collect.Lists;

/**
 * This router can starve out other groups.  If there are always high priority items, it will
 * never execute low priority items
 * 
 * TODO: I feel like the immutable state of the queue groups is making routing
 *       inefficient
 * 
 * @author jclawson
 *
 * @param <T>
 */
public class LoadBalancedRouter<T> implements ListRouter<T> {

	private final Comparator<T> comparator;
	private final List<T> list;
    private final Callable<List<T>> fetchList;
    private RouteCondition<T> condition;
	
    public static <T2> LoadBalancedRouter<T2> newInstance(List<T2> list, Comparator<T2> comparator) {
        return new LoadBalancedRouter<T2>(list, comparator);
    }
    
	public LoadBalancedRouter(List<T> list, Comparator<T> comparator) {
		this.comparator = comparator;
		this.list = list;
		this.fetchList = null;
	}
	
	public LoadBalancedRouter(Callable<List<T>> fetchList, Comparator<T> comparator) {
		this.comparator = comparator;
		this.fetchList = fetchList;
		this.list = null;
	}
	
	private List<T> getList(){
        try {
            List<T> myList = Lists.newLinkedList();
            
            //filter out non-routable routes.  This sucks that we have to do this
            for(T route : (list == null) ? fetchList.call() : list) {
                if(condition.isRoutable(route)) {
                    myList.add(route);
                }
            }
            
            return myList;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
	
	
	public T next() {
		//find least weight
		T current = null;
		Iterator<T> it = getList().iterator();	
			while(it.hasNext()) {
				T next = it.next();
				if(condition != null && !condition.isRoutable(next))
				    next = null;
				
				if(current == null) {
				    current = next;
				} else if(next != null) {
    				int comparison = comparator.compare(next, current);
    				//if next is less, or its equal and we have a 50% chance to swap
    				if(comparison < 0 || (comparison == 0 && Math.random() > .5)) {
    					current = next;
    				}
				}
			}
		return current;
	}

    public void setRouteCondition(RouteCondition<T> condition) {
        this.condition = condition;
    }
	
}
