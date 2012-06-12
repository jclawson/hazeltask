package com.succinctllc.core.collections;

import java.util.AbstractList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A CopyOnWriteArraySet that also allows some List functionality, like getting by index
 * 
 * @author jclawson
 *
 * @param <E>
 */
public class CopyOnWriteArrayListSet<E> extends AbstractList<E> implements Set<E> {
    private final CopyOnWriteArrayList<E> arrayList;

    public CopyOnWriteArrayListSet(){
        arrayList = new CopyOnWriteArrayList<E>();
    }
    
    public CopyOnWriteArrayListSet(Collection<E> init){
        arrayList = new CopyOnWriteArrayList<E>(init);
    }
    
    public boolean add(E element) {
        return arrayList.addIfAbsent(element);
    }

    public E remove(int index) {
        return arrayList.remove(index);
    }

    public int size() {
        return arrayList.size();
    }

    public E get(int index) {
        return arrayList.get(index);
    }
}
