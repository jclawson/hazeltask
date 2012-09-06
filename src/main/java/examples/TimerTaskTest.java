package examples;

import java.util.Timer;
import java.util.TimerTask;

public class TimerTaskTest {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Timer t = new Timer();
        t.schedule(new Task(),0,5000);
        
        Thread.currentThread().join();
    }
    
    public static class Task extends TimerTask {
        private static long lastTime = 0;
        
        @Override
        public void run() {
            
            double seconds = 0;
            if(lastTime != 0) {
                seconds = Math.round((System.currentTimeMillis() - lastTime)*100/1000)/100;
            }
            
            
            
            System.out.println("working... ("+seconds+"s)");
            if(seconds > 1) {
                
            
                try {
                    Thread.currentThread().sleep(5000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                System.out.println("done...");
            } else {
                System.out.println("skip");
            }
            lastTime = System.currentTimeMillis();
            
        }
        
    }

}
