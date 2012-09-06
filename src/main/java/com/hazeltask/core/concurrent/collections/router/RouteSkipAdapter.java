package com.hazeltask.core.concurrent.collections.router;

/**
 * Allows you to specify custom logic in order to skip an item
 * selected by the router.  It is up to the router to decide what 
 * to do if you indicate it should skip.  Typically it should call next()
 * again, and employ a mechanism to make sure it doesn't do an infinite loop
 * @author jclawson
 *
 * @param <T>
 */
public interface RouteSkipAdapter<T> {
    public boolean shouldSkip(T item);
}
