package com.orchestra.workflow;

public class RetryPolicy {

    private final long initialDelayMillis;

    public RetryPolicy(long initialDelayMillis) {
        this.initialDelayMillis = initialDelayMillis;
    }

    public long getDelayMillis(int retryCount) {
        return initialDelayMillis * (1L << (retryCount - 1));
    }
}