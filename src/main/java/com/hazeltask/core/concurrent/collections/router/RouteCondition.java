package com.hazeltask.core.concurrent.collections.router;

/**
 * Allows the specification of a confition that a route must meet in order to 
 * by chosen for routing.
 * @author jclawson
 *
 * @param <T>
 */
public interface RouteCondition<T> {
    boolean isRoutable(T route);
}
