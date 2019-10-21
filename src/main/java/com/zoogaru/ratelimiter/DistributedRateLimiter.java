package com.zoogaru.ratelimiter;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DistributedRateLimiter extends ScheduledThreadPoolExecutor {

    private Object available = new Object();

    // Shared data between the cluster's node
    private Integer delay = 0 ;

    DistributedRateLimiter() throws Exception {
        super(1);
        this.setRemoveOnCancelPolicy(false);
    }

    DistributedRateLimiter(String nodeName) throws Exception {
        this();
    }

    DistributedRateLimiter(String nodeName, int time) throws Exception {
        this(nodeName);//call default
        this.delay = time ;
    }

    ScheduledFuture<?> schedule(Request cmd) {
        System.out.println(new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()));
        return schedule(cmd, 0, TimeUnit.SECONDS);
    }

    ScheduledFuture<?> schedule(CombinableRequest cmd) {
        if(cmd.isCombinable()) {
        }
        System.out.println(new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()));
        return schedule(cmd, 0, TimeUnit.SECONDS);
    }

    protected void afterExecute(Runnable r, Throwable t) {
        synchronized (available) {
            try {
                available.wait(delay * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Execution left for " + getQueue().size());
    }
}
