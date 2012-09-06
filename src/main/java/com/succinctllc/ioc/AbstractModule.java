package com.succinctllc.ioc;

public abstract class AbstractModule {

    private Injector i;
    
    public final <T> void bind(Class<T> classInterface, Class<? extends T> classImpl) {
        i.bind(classInterface, classImpl);
    }

    public final <T> void bind(Class<T> classInterface, T instance) {
        i.bind(classInterface, instance);
    }
    
    public final <T> void bind(Class<T> classInterface, String named, Class<? extends T> classImpl) {
        i.bind(classInterface, classImpl, named);
    }

    public final <T> void bind(Class<T> classInterface, String named, T instance) {
        i.bind(classInterface, instance, named);
    }
    
    public final void injectMembers(Object o) {
        i.injectMembers(o);
    }
    
    public final void install(AbstractModule m) {
        i.install(m);
    }
    
    protected final void configure(Injector i) {
        this.i = i;
        this.configure();
    }
    
    public abstract void configure();

}
