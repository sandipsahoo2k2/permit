package com.zoogaru.ratelimiter;

import java.util.concurrent.Future;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
        SimplePermitCoordinator coord = new SimplePermitCoordinator("Sandeep-" + Math.random());
        Future<Boolean> permitAvailable = coord.requestPermit();
        boolean available = permitAvailable.get() ;
        while(!available) {
            permitAvailable = coord.requestPermit();
            available = permitAvailable.get() ;
        }
        if(coord.hasPermit()) {

            int waitTime = 5 ; //second
            SimpleRateLimiter limiter = new SimpleRateLimiter(waitTime);
            int totalTask = 5 ;

            for (int i = 0; i < totalTask; i++) {
                final int l = i;
                limiter.execute(() -> {
                    System.out.println("ACTING LEADER Hello World " + l);
                });
            }

            Thread.sleep(waitTime * totalTask * 1002);
            limiter.shutdown();

            if(limiter.getQueueSize() == 0) {
                coord.returnPermit();
            }
        } else {
            System.out.println("Future completed");
        }
    }
}
