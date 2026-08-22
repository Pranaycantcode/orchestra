package com.orchestra.workflow;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class TaskRetryTest {

    @Test
    void shouldAllowRetryWhenRetriesRemain() {

        Task task = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of()
        );

        task.setMaxRetries(3);
        task.setRetryCount(1);

        assertTrue(task.canRetry());
    }

    @Test
    void shouldNotAllowRetryWhenRetriesAreExhausted() {

        Task task = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of()
        );

        task.setMaxRetries(3);
        task.setRetryCount(3);

        assertFalse(task.canRetry());
    }

    @Test
    void shouldStartWithZeroRetries() {

        Task task = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of()
        );

        assertEquals(
                0,
                task.getRetryCount()
        );

        assertEquals(
                0,
                task.getMaxRetries()
        );

        assertFalse(task.canRetry());
    }
}