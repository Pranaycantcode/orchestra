package com.orchestra.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

class TaskExecutionRequestTest {

    @Test
    void shouldCreateExecutionRequest() {

        TaskExecutionRequest request =
                new TaskExecutionRequest(
                        "A",
                        "echo A",
                        2);

        assertEquals("A", request.taskId());
        assertEquals("echo A", request.command());
        assertEquals(2, request.retryCount());
    }
}