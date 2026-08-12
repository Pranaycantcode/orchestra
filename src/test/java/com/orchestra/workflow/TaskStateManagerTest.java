package com.orchestra.workflow;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TaskStateManagerTest {

    @Test
    void shouldMarkTaskRunning() {

        Task task = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of()
        );

        TaskStateManager stateManager =
                new TaskStateManager();

        stateManager.markRunning(task);

        assertEquals(
                TaskStatus.RUNNING,
                task.getStatus()
        );
    }

    @Test
    void shouldMarkTaskSuccessful() {

        Task task = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of()
        );

        TaskStateManager stateManager =
                new TaskStateManager();

        stateManager.markSuccess(task);

        assertEquals(
                TaskStatus.SUCCESS,
                task.getStatus()
        );
    }

    @Test
    void shouldMarkTaskFailed() {

        Task task = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of()
        );

        TaskStateManager stateManager =
                new TaskStateManager();

        stateManager.markFailed(task);

        assertEquals(
                TaskStatus.FAILED,
                task.getStatus()
        );
    }

    @Test
    void shouldMarkTaskBlocked() {

        Task task = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of()
        );

        TaskStateManager stateManager =
                new TaskStateManager();

        stateManager.markBlocked(task);

        assertEquals(
                TaskStatus.BLOCKED,
                task.getStatus()
        );
    }
}