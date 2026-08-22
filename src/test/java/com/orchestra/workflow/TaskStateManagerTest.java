package com.orchestra.workflow;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TaskStateManagerTest {

        @Test
        void shouldMarkTaskRunning() {

                Task task = new Task(
                                "A",
                                "Task A",
                                "echo A",
                                Set.of());

                TaskStateManager stateManager = new TaskStateManager();

                stateManager.markRunning(task);

                assertEquals(
                                TaskStatus.RUNNING,
                                task.getStatus());
        }

        @Test
        void shouldMarkTaskSuccessful() {

                Task task = new Task(
                                "A",
                                "Task A",
                                "echo A",
                                Set.of());

                TaskStateManager stateManager = new TaskStateManager();

                stateManager.markRunning(task);
                stateManager.markSuccess(task);

                assertEquals(
                                TaskStatus.SUCCESS,
                                task.getStatus());
        }

        @Test
        void shouldMarkTaskFailed() {

                Task task = new Task(
                                "A",
                                "Task A",
                                "echo A",
                                Set.of());

                TaskStateManager stateManager = new TaskStateManager();

                stateManager.markRunning(task);
                stateManager.markFailed(task);

                assertEquals(
                                TaskStatus.FAILED,
                                task.getStatus());
        }

        @Test
        void shouldMarkTaskBlocked() {

                Task task = new Task(
                                "A",
                                "Task A",
                                "echo A",
                                Set.of());

                TaskStateManager stateManager = new TaskStateManager();

                stateManager.markRunning(task);
                stateManager.markBlocked(task);

                assertEquals(
                                TaskStatus.BLOCKED,
                                task.getStatus());
        }

        @Test
        void shouldAllowRetryFromFailedToPending() {

                Task task = new Task(
                                "A",
                                "Task A",
                                "echo A",
                                Set.of());

                TaskStateManager stateManager = new TaskStateManager();

                stateManager.markRunning(task);
                stateManager.markFailed(task);
                stateManager.markPending(task);

                assertEquals(
                                TaskStatus.PENDING,
                                task.getStatus());
        }

        @Test
        void shouldRejectTransitionFromSuccessToRunning() {

                Task task = new Task(
                                "A",
                                "Task A",
                                "echo A",
                                Set.of());

                TaskStateManager stateManager = new TaskStateManager();

                stateManager.markRunning(task);
                stateManager.markSuccess(task);

                assertThrows(
                                IllegalStateException.class,
                                () -> stateManager.markRunning(task));
        }
}