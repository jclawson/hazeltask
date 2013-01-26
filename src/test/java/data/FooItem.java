package data;

import java.io.Serializable;

public class FooItem implements Serializable {
    private static final long serialVersionUID = 1L;
    public int id;
    public FooItem(int id) {
        this.id = id;
    }
    
    public FooItem(){}
}
