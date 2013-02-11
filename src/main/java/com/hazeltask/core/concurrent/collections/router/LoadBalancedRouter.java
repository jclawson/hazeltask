package com.hazeltask.core.concurrent.collections.router;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * TODO: make this more efficient... its not good enough that we find the heaviest weight...
 * We need to find the heaviest weight that has messages in the list... ie that we don't wish to skip
 * 
 * Perhaps the fetchList callback needs to filter out groups that have no items beforehand
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
            return (list == null) ? fetchList.call() : list;
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
