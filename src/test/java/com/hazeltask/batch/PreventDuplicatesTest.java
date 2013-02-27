package com.hazeltask.batch;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

import com.hazeltask.executor.task.HazeltaskTask;

import data.FooItem;

public class PreventDuplicatesTest {
    
    @Test
    public void testAllowAdd() {
        DefaultBatchKeyAdapter<FooItem> batchKeyAdapter = new DefaultBatchKeyAdapter<FooItem>();
        @SuppressWarnings("unchecked")
        IBatchClusterService<FooItem, String, String> svc = mock(IBatchClusterService.class);
        when(svc.isInPreventDuplicateSet(anyString()))
            .thenReturn(false);
        
        PreventDuplicatesListener<FooItem, String> listener = new PreventDuplicatesListener<FooItem, String>(svc, batchKeyAdapter);
        Assert.assertTrue(listener.beforeAdd(new FooItem()));
    }
    
    @Test
    public void testDenyAdd() {
        DefaultBatchKeyAdapter<FooItem> batchKeyAdapter = new DefaultBatchKeyAdapter<FooItem>();
        @SuppressWarnings("unchecked")
        IBatchClusterService<FooItem, String, String> svc = mock(IBatchClusterService.class);
        when(svc.isInPreventDuplicateSet(anyString()))
            .thenReturn(true);
        
        PreventDuplicatesListener<FooItem, String> listener = new PreventDuplicatesListener<FooItem, String>(svc, batchKeyAdapter);
        Assert.assertFalse(listener.beforeAdd(new FooItem()));
    }
    
    @Test
    public void testRemoveAfterExecute() {
        FooItem item = new FooItem();
        DefaultBatchKeyAdapter<FooItem> batchKeyAdapter = new DefaultBatchKeyAdapter<FooItem>();
        @SuppressWarnings("unchecked")
        IBatchClusterService<FooItem, String, String> svc = mock(IBatchClusterService.class);
        when(svc.addToPreventDuplicateSet(anyString()))
            .thenReturn(true);
        
        
        TaskBatch<FooItem, String, String> bundle = mock(TaskBatch.class);
        when(bundle.getItems()).thenReturn(Arrays.asList(item));
        
        HazeltaskTask work = mock(HazeltaskTask.class);
        when(work.getInnerRunnable()).thenReturn(bundle);
        
        PreventDuplicatesListener<FooItem, String> listener = new PreventDuplicatesListener<FooItem, String>(svc, batchKeyAdapter);
        listener.afterExecute(work, null);
    }
}
