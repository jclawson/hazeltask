package data;

import com.hazeltask.core.concurrent.collections.grouped.Groupable;

public class MyGroupableItem implements Groupable<Long> {
    private final long group;
    public MyGroupableItem(long group) {
        this.group = group;
    }
    
    @Override
    public Long getGroup() {
        return group;
    }
}
