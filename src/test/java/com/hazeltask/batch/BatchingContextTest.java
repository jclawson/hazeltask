package com.hazeltask.batch;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;
import org.mockito.InOrder;

import com.hazeltask.config.BundlerConfig;
import com.hazeltask.executor.DistributedExecutorService;

import data.FooItem;

public class BatchingContextTest {
    @Test
    public void testBasic() {
        DistributedExecutorService svc = mock(DistributedExecutorService.class);
        BundlerConfig<FooItem, String, String, String> cfg = mock(BundlerConfig.class);
        
        when(cfg.getBundler()).thenReturn(new BatchFactory());
        when(cfg.getBatchKeyAdapter()).thenReturn(new DefaultBatchKeyAdapter<FooItem>());
        when(cfg.getFlushSize()).thenReturn(10);
        
        BatchingContext<FooItem> ctx = new BatchingContext<FooItem>(svc, cfg);
        
        for(int i=1; i<=15; i++) {
            ctx.addItem(new FooItem(i));
        }
        
        ctx.submit();
        
        InOrder svcOrder = inOrder(svc);
        svcOrder.verify(svc).execute(argThat(new ItemsMatches(10)));
        svcOrder.verify(svc).execute(argThat(new ItemsMatches(5)));
    }
    
    
    public static class BatchFactory implements IBatchFactory<FooItem, String, String> {
        @Override
        public TaskBatch<FooItem, String, String> createBatch(String group,  Collection<FooItem> items) {
            return new TaskBatchImpl(UUID.randomUUID().toString(), group, items);
        }    
    }
    
    public static class TaskBatchImpl extends AbstractTaskBatch<FooItem, String, String> {

        public TaskBatchImpl(String id, String group, Collection<FooItem> items) {
            super(id, group, items);
        }

        @Override
        public void run(List<FooItem> items) {
            // TODO Auto-generated method stub
            
        }
    }
    
    public static class ItemsMatches extends BaseMatcher<Runnable> {

        private int count;
        public ItemsMatches(int count) {
            this.count = count;
        }
        
        public boolean matches(Object item) {
            if(item instanceof TaskBatch) {
                return ((TaskBatch) item).getItems().size() == count;
            }
            return false;
        }

        public void describeTo(Description description) {
            
        }
        
    }
    
    @Test
    public void testMultiGroup() {
     // TODO testMultiGroup
    }
}
