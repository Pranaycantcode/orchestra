package com.orchestra.execution;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RetryScheduler {

    private final ScheduledExecutorService scheduler;

    public RetryScheduler(
            ScheduledExecutorService scheduler) {

        this.scheduler = scheduler;
    }

    public void schedule(
            Runnable task,
            long delayMillis) {

        scheduler.schedule(
                task,
                delayMillis,
                TimeUnit.MILLISECONDS
        );
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}