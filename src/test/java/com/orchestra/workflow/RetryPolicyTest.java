package com.orchestra.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {

    @Test
    void shouldCalculateExponentialBackoff() {

        RetryPolicy policy =
                new RetryPolicy(100);

        assertEquals(
                100,
                policy.getDelayMillis(1)
        );

        assertEquals(
                200,
                policy.getDelayMillis(2)
        );

        assertEquals(
                400,
                policy.getDelayMillis(3)
        );

        assertEquals(
                800,
                policy.getDelayMillis(4)
        );
    }
}