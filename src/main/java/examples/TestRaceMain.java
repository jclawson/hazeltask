package examples;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import com.succinctllc.hazelcast.work.router.ListRouter;
import com.succinctllc.hazelcast.work.router.RoundRobinRouter;


public class TestRaceMain {

    /**
     * @param args
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws InterruptedException {
        new TestRaceMain();
        
        

    }
    
    public static List<String> stats = new Vector<String>();
    
    public volatile boolean run = true;
    
    public TestRaceMain() throws InterruptedException{
        Thread t1 = new Thread(new MyRun());
        Thread t2 = new Thread(new MyRun());
        Thread t3 = new Thread(new MyInc());
        Thread t4 = new Thread(new MyRun());
        Thread t5 = new Thread(new MyRun());
        Thread t6 = new Thread(new MyRun());
        Thread t7 = new Thread(new MyRun());
        //Thread t8 = new Thread(new MyInc());
        Thread t9 = new Thread(new MyRun());
        Thread t10 = new Thread(new MyRun());
        
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();
        t6.start();
        t7.start();
       // t8.start();
        t9.start();
        t10.start();
        
        Thread.currentThread().sleep(500);
        run = false;
        Thread.currentThread().sleep(100);
        
        ConcurrentHashMap<String, Integer> results = new ConcurrentHashMap<String,Integer>();
        
        for(String r : stats) {
            results.putIfAbsent(r, 0);
            //System.out.println("put "+results.get(r));
            results.put(r, results.get(r)+1);
        }
        
        System.out.println("\nTOTAL: "+stats.size());
        long otherTotal = 0;
        long origTotal = 0;
        for(Entry<String, Integer> e : results.entrySet()) {
            if(!e.getKey().startsWith("new")) {
                System.out.println(e.getKey()+": "+e.getValue());
                origTotal += e.getValue();
            } else {
                System.out.println(" "+e.getKey()+": "+e.getValue());
                otherTotal += e.getValue();
            }
        }
        System.out.println("Orig total: "+origTotal);
        System.out.println("Other total: "+otherTotal);
        System.out.println(stats.size()+" == "+(origTotal+otherTotal));
        System.out.println("Orig %: "+Math.round((((double)origTotal /  (double)stats.size())*100))+"%");
    }
    
    List<String> list = Collections.synchronizedList(new LinkedList<String>(Arrays.asList("Jason", "Jenny", "Gizmo", "Steve")));
    ListRouter<String> router = new RoundRobinRouter(list);
    
    public class MyInc  implements Runnable {
        public void run() {
            int i = 0;
            while(i++ < 100 && run) {
               if(i % 10 == 0) {
                   list.add("new-"+i);
               }
               
               if(false && i % 15 == 0) {
                   synchronized(list) {
                  Iterator<String> it = list.iterator();
                  String v = null;    
                  while(it.hasNext()) {
                          String c = it.next();
                          if(c.startsWith("new"))
                              v = c;
                      }
                  
                  if(v != null)
                      list.remove(v);
                   }
               }
               
               try {
                Thread.sleep(4);
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
            
            while(i++ < 1200000 && run) {
                String result = router.next();
                stats.add(result);
                
            }
            System.out.println("done");
        }
        
    }

}
