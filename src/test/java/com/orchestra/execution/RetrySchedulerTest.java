package com.orchestra.execution;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class RetrySchedulerTest {

    @Test
    void shouldScheduleTaskAfterDelay()
            throws Exception {

        var schedulerService =
                Executors.newScheduledThreadPool(1);

        RetryScheduler retryScheduler =
                new RetryScheduler(
                        schedulerService
                );

        CountDownLatch latch =
                new CountDownLatch(1);

        long start =
                System.currentTimeMillis();

        try {

            retryScheduler.schedule(
                    latch::countDown,
                    100
            );

            assertTrue(
                    latch.await(
                            1,
                            TimeUnit.SECONDS
                    )
            );

            long elapsed =
                    System.currentTimeMillis()
                            - start;

            assertTrue(
                    elapsed >= 100
            );

        } finally {

            retryScheduler.shutdown();
        }
    }
}