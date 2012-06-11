import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import com.succinctllc.executor.router.ListRouter;
import com.succinctllc.executor.router.RoundRobinRouter;


public class TestRaceMain {

    /**
     * @param args
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws InterruptedException {
        new TestRaceMain();
        
        

    }
    
    public static List<String> stats = new Vector<String>();
    
    public TestRaceMain() throws InterruptedException{
        Thread t1 = new Thread(new MyRun());
        Thread t2 = new Thread(new MyRun());
        Thread t3 = new Thread(new MyInc());
        Thread t4 = new Thread(new MyRun());
        Thread t5 = new Thread(new MyRun());
        
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();
        
        Thread.currentThread().sleep(1000);
        
        ConcurrentHashMap<String, Integer> results = new ConcurrentHashMap<String,Integer>();
        for(String r : stats) {
            results.putIfAbsent(r, 0);
            //System.out.println("put "+results.get(r));
            results.put(r, results.get(r)+1);
        }
        
        System.out.println(stats.size());
        for(Entry<String, Integer> e : results.entrySet()) {
            System.out.println(e.getKey()+": "+e.getValue());
        }
    }
    
    List<String> list = new Vector(Arrays.asList("Jason", "Jenny", "Gizmo"));
    ListRouter<String> router = new RoundRobinRouter(list);
    
    public class MyInc  implements Runnable {
        public void run() {
            int i = 0;
            while(i++ < 10) {
               if(i % 3 == 0) {
                    list.add("new-"+i);
               }
               
               if(i % 7 == 0) {
                   list.remove(1);
               }
               
               try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            }
        }
    }
    
    public class MyRun implements Runnable {

        public void run() {
            int i = 0;
            
            while(i++ < 100000) {
                String result = router.next();
                stats.add(result);
                
            }
            System.out.println("done");
        }
        
    }

}
