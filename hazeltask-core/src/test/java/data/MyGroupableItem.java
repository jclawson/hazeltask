package data;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazeltask.core.concurrent.collections.grouped.Groupable;
import com.hazeltask.core.concurrent.collections.tracked.TrackCreated;

public class MyGroupableItem implements Groupable<Long>, TrackCreated {
    public final long group;
    public final long time;
    public final long id;
    
    private static AtomicInteger ai = new AtomicInteger();
    
    public MyGroupableItem(long group) {
        this.group = group;
        this.time = System.currentTimeMillis();
        this.id = ai.incrementAndGet();
    }
    
    public MyGroupableItem(long id, long group) {
        this.group = group;
        this.time = System.currentTimeMillis();
        this.id = id;
    }
    
    @Override
    public Long getGroup() {
        return group;
    }

    @Override
    public long getTimeCreated() {
        return time;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id ^ (id >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        MyGroupableItem other = (MyGroupableItem) obj;
        if (id != other.id) return false;
        return true;
    }

    @Override
    public String toString() {
        return "g:"+group+"\tid:"+id;
    }
    
    
}
