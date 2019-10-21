package com.zoogaru.ratelimiter;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SimpleRateLimiter extends ScheduledThreadPoolExecutor {
    private long time = 0 ;
    private Object available = new Object();

    SimpleRateLimiter() {
        super(1);
    }

    SimpleRateLimiter(long time) {
        super(1);
        this.time = time ;
    }

    SimpleRateLimiter(long time, boolean value) {
        super(1);
        this.time = time ;
        this.setRemoveOnCancelPolicy(value);
    }

    ScheduledFuture<?> schedule(Request cmd) {
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
                available.wait(time * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Execution left for " + getQueue().size());
    }

    public int getQueueSize() {
        return super.getQueue().size();
    }
}
