package com.succinctllc.ioc;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

/**
 * Very, very, very brain dead simple dependency injection so I don't need to
 * rely on a lot of external libraries
 * 
 * TODO: make weak references
 * 
 * @author jclawson
 */
public class Injector {

    private Map<Key, Class<?>> iterfaceToClassMappings          = new HashMap<Key, Class<?>>();
    private Map<Key, Object>   iterfaceToInstanceMappings       = new HashMap<Key, Object>();
    private Map<Key, Object>   iterfaceToClassSingletonMappings = new HashMap<Key, Object>();
    private List<Object>            objectsNeedingInject             = new ArrayList<Object>();
    private List<AbstractModule>    modules                          = new ArrayList<AbstractModule>();

    public static class Key {
        Class<?> clazz;
        String named = null;
        
        public Key(String name, Class<?> clazz) {
            this.named = name;
            this.clazz = clazz;
        }
        
        public Key(Class<?> clazz) {
            this.clazz = clazz;
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            String className = clazz.getName();
            result = prime * result + className.hashCode();
            result = prime * result + ((named == null) ? 0 : named.hashCode());
            return result;
        }
        
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            Key other = (Key) obj;
            
            String className = clazz.getName();
            if (!className.equals(other.clazz.getName())) return false;
            
            if (named == null) {
                if (other.named != null) return false;
            } else if (!named.equals(other.named)) return false;
            return true;
        }
        
        
    }
    
    public static Injector createInjector(AbstractModule module) {
        return new Injector(module);
    }

    private Injector(AbstractModule module) {
        module.configure(this);
        
        for(AbstractModule m : modules) {
            m.configure(this);
        }
        
        for (Object o : objectsNeedingInject) {
            try {
                doInjectMembers(o);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public <T> T getInstance(Class<T> cls, String named) {
        Key key = new Key(named, cls);
        return getInstance(cls, key);
    }
    
    public <T> T getInstance(Class<T> cls) {
        Key key = new Key(cls);
        return getInstance(cls, key);
    }

    @SuppressWarnings("unchecked")
    public <T> T getInstance(Class<T> cls, Key key) {        
        T t = (T) iterfaceToClassSingletonMappings.get(key);
        if (t == null) {
            t = (T) iterfaceToInstanceMappings.get(key);
            if (t == null) {
                Class<? extends T> implCls = (Class<? extends T>) iterfaceToClassMappings.get(key);
                try {
                    boolean isSingleton = false;
                    if (implCls == null) {
                        t = cls.newInstance();
                        isSingleton = cls.getAnnotation(Singleton.class) != null;
                    } else {
                        t = implCls.newInstance();
                        isSingleton = cls.getAnnotation(Singleton.class) != null
                                || implCls.getAnnotation(Singleton.class) != null;
                    }
                    doInjectMembers(t);

                    if (isSingleton) iterfaceToClassSingletonMappings.put(key, t);

                } catch (InstantiationException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return t;
    }

    protected final void injectMembers(Object instance) {
        // schedule members for injection
        objectsNeedingInject.add(instance);
    }

    private final void doInjectMembers(Object instance) throws IllegalArgumentException, IllegalAccessException {
        //get all fields marked @Inject, find their instances, set them
        Collection<Field> fields = getInheritedDeclatedFields(instance.getClass(), Inject.class);
        for(Field f : fields) {
            f.setAccessible(true);
            Object v;
            Named named = f.getAnnotation(Named.class);
            if(named != null) {
                v = getInstance(f.getType(), named.value());
            } else {
                v = getInstance(f.getType());
            }
            f.set(instance, v);
        }
    }

    private Collection<Field> getInheritedDeclaredFields(Class<?> cls) {
        Class<?> currentCls = cls;
        List<Field> fields = new ArrayList<Field>();
        while (currentCls != null) {
            fields.addAll(Arrays.asList(currentCls.getDeclaredFields()));
            currentCls = currentCls.getSuperclass();
        }

        return fields;
    }

    private Collection<Field> getInheritedDeclatedFields(Class<?> cls,
            final Class<? extends Annotation> withAnnotation) {
        Collection<Field> fields = getInheritedDeclaredFields(cls);
        Collection<Field> field = Collections2.filter(fields, new Predicate<Field>() {
            public boolean apply(Field field) {
                return field.getAnnotation(withAnnotation) != null;
            }
        });
        return field;
    }
    
    public final void install(AbstractModule m) {
        modules.add(m);
    }

    protected final void bind(Class<?> classInterface, Class<?> classImpl) {
        iterfaceToClassMappings.put(new Key(classInterface), classImpl);
    }

    protected final <T> void bind(Class<T> classInterface, T instance) {
        iterfaceToInstanceMappings.put(new Key(classInterface), instance);
    }
    
    protected final void bind(Class<?> classInterface, Class<?> classImpl, String named) {
        iterfaceToClassMappings.put(new Key(named, classInterface), classImpl);
    }

    protected final <T> void bind(Class<T> classInterface, T instance, String named) {
        iterfaceToInstanceMappings.put(new Key(named, classInterface), instance);
    }
}
