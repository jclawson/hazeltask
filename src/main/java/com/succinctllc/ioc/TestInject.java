package com.succinctllc.ioc;

public class TestInject {

    /**
     * @param args
     */
    public static void main(String[] args) {
        Injector i = Injector.createInjector(new MyModule());
        IFoo foo = i.getInstance(IFoo.class);
        System.out.println(foo);
        foo = i.getInstance(IFoo.class);
        System.out.println(foo);
    }
    
    public static class MyModule extends AbstractModule {
        @Override
        public void configure() {
            bind(String.class, "Jason");
            bind(String.class, "love", "Jenny");
            bind(IFoo.class, Foo.class);
        }    
    }

    
    public static interface IFoo {
        
    }
    
    public static class Bar {
        @Inject
        String name;
        
        @Inject
        @Named("love")
        String love;
    }
    
    //@Singleton
    public static class Foo implements IFoo {
        @Inject
        Bar bar;
        
        public static int i;
        
        public Foo(){
            i++;
        }
        
        public String toString() {
            return bar.name+" "+i+" loves "+bar.love;
        }
    }
    
}
