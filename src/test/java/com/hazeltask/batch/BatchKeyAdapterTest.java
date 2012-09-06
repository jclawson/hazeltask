package com.hazeltask.batch;

import junit.framework.Assert;

import org.junit.Test;

import data.FooItem;

public class BatchKeyAdapterTest {
    
    @Test
    public void testItemId() {
        DefaultBatchKeyAdapter<FooItem> key = new DefaultBatchKeyAdapter<FooItem>();
        FooItem f1 = new FooItem();
        String k1 = key.getItemId(f1);
        String k2 = key.getItemId(f1);
        
        Assert.assertNotSame(k1, k2);
    }
    
    @Test
    public void testItemGroup() {
        DefaultBatchKeyAdapter<FooItem> key = new DefaultBatchKeyAdapter<FooItem>();
        FooItem f1 = new FooItem();
        String g1 = key.getItemGroup(f1);
        String g2 = key.getItemGroup(f1);
        Assert.assertEquals(g1, g2);
    }
}
