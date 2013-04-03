package data;

import java.io.Serializable;

import com.hazeltask.core.concurrent.collections.tracked.TrackCreated;

public class SimpleItem implements TrackCreated, Serializable {
    public long timeCreated;
    public int id;
    public SimpleItem(long timeCreated) {
        this.id = (int) timeCreated;
        this.timeCreated=timeCreated;
    }
    
    public SimpleItem(int id, long timeCreated) {
        this.id = id;
        this.timeCreated=timeCreated;
    }
    
    @Override
    public long getTimeCreated() {
        return timeCreated;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + (int) (timeCreated ^ (timeCreated >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        SimpleItem other = (SimpleItem) obj;
        if (id != other.id) return false;
        if (timeCreated != other.timeCreated) return false;
        return true;
    }
    
    

}
