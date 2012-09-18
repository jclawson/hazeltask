package com.hazeltask.hazelcast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.MultiMap;

/**
 * This MultiMap implementation presents a consistent interface for using a MultiMap backed by
 * Guava or Hazelcast just to make some code a little cleaner.  It might be nice to make this better
 * and use ReentrantReadWriteLocks to sychnonize the guava collections.  Then we can implement
 * things nicer.
 * 
 * @author Jason Clawson
 *
 */
public class MultiMapProxy<K, V> /*implements MultiMap<K, V>*/ {
	private final MultiMap<K, V> hcMultiMap;
	private final SetMultimap<K, V> guavaMultiMap;
	
	private MultiMapProxy(MultiMap<K, V> hcMultiMap) {
		this.hcMultiMap = hcMultiMap;
		this.guavaMultiMap = null;
	}
	
	private MultiMapProxy(SetMultimap<K, V> guavaMultiMap) {
		this.guavaMultiMap = guavaMultiMap;
		this.hcMultiMap = null;
	}
	
	public static <K,V> MultiMapProxy<K,V> localMultiMap(SetMultimap<K, V> guavaMultiMap) {
		return new MultiMapProxy<K,V>(Multimaps.<K,V>synchronizedSetMultimap(guavaMultiMap));
	}
	
	public static <K,V> MultiMapProxy<K,V> clusteredMultiMap(MultiMap<K, V> hcMultiMap) {
		return new MultiMapProxy<K,V>(hcMultiMap);
	}

	public boolean put(K key, V value) {
		if(guavaMultiMap != null)
			return guavaMultiMap.put(key, value);
		return hcMultiMap.put(key, value);
	}

	public Collection<V> get(K key) {
		if(guavaMultiMap != null) {
			synchronized (guavaMultiMap) {
				Set<V> c = guavaMultiMap.get(key);
				if(c != null)
					return new HashSet<V>(c);
				else
					return null;
			}			
		}
		return hcMultiMap.get(key);
	}
	
	public List<V> getAsList(K key) {
		if(guavaMultiMap != null) {
			synchronized (guavaMultiMap) {
				Collection<V> c = guavaMultiMap.get(key);
				if(c != null)
					return new ArrayList<V>(c);
				else
					return null;
			}			
		} else {
			Collection<V> c = hcMultiMap.get(key);
			if(c != null)
				return new ArrayList<V>(c);
			else
				return null;
		}
	}

	public boolean remove(Object key, Object value) {
		if(guavaMultiMap != null)
			return guavaMultiMap.remove(key, value);
		return hcMultiMap.remove(key, value);
	}
	
	public int removeAll(Object key, Collection<Object> values) {
        int totalRemoved = 0;
	    if(guavaMultiMap != null) {
            synchronized (guavaMultiMap) {
                for(Object o : values) {
                    if(guavaMultiMap.remove(key, o))
                        totalRemoved ++;
                }
            } 
        } else {
            for(Object o : values) {
                if(hcMultiMap.remove(key, o))
                    totalRemoved ++;
            }
        }
	    return totalRemoved;
    }

	public Collection<V> remove(Object key) {
		return hcMultiMap.remove(key);
	}

	public Set<K> localKeySet() {
		if(guavaMultiMap != null)
			return keySet();
		return hcMultiMap.localKeySet();
	}

	public Set<K> keySet() {
		if(guavaMultiMap != null) {
			synchronized (guavaMultiMap) {
				Set<K> c = guavaMultiMap.keySet();
				if(c != null)
					return new HashSet<K>(c);
				else
					return null;
			}
		}
		return hcMultiMap.keySet();
	}

	public Collection<V> values() {
		if(guavaMultiMap != null) {
			synchronized (guavaMultiMap) {
				Collection<V> c = guavaMultiMap.values();
				if(c != null)
					return new ArrayList<V>(c);
				else
					return null;
			}
		}
		return hcMultiMap.values();
	}

//don't feel like figuring out what to do with guava synchronization right now plus don't need it
//	public Set<Entry<K, V>> entrySet() {
//		if(guavaMultiMap != null)
//			return guavaMultiMap.entries();
//		return hcMultiMap.entrySet();
//	}

	public boolean containsKey(K key) {
		if(guavaMultiMap != null)
			return guavaMultiMap.containsKey(key);
		return hcMultiMap.containsKey(key);
	}

	public boolean containsValue(Object value) {
		if(guavaMultiMap != null)
			return guavaMultiMap.containsValue(value);
		return hcMultiMap.containsValue(value);
	}

	public boolean containsEntry(K key, V value) {
		if(guavaMultiMap != null)
			return guavaMultiMap.containsEntry(key, value);
		return hcMultiMap.containsEntry(key, value);
	}

	public int size() {
		if(guavaMultiMap != null)
			return guavaMultiMap.size();
		return hcMultiMap.size();
	}

	public void clear() {
		if(guavaMultiMap != null)
			guavaMultiMap.clear();
		else
			hcMultiMap.clear();
	}

	
	
}
