package com.hazeltask;

/**
 * This is a local listener for getting events on the state of executor service and batching service
 * @author jclawson
 *
 */
public class HazeltaskServiceListener<T extends ServiceListenable> {
    public void onBeginStart(T svc){};
    public void onEndStart(T svc){};
    public void onBeginShutdown(T svc){};
    public void onEndShutdown(T svc){};
}
