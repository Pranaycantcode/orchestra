package com.orchestra.execution;

import java.util.Set;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import com.orchestra.workflow.Task;
import com.orchestra.workflow.TaskStatus;

class TaskExecutionPoolTest {

        @Test
        void shouldExecuteTasksUsingMultipleThreads()
                        throws Exception {

                Task taskA = new Task(
                                "A",
                                "Task A",
                                "sleep",
                                Set.of());

                Task taskB = new Task(
                                "B",
                                "Task B",
                                "sleep",
                                Set.of());

                TaskExecutor executor = task -> {

                        task.setStatus(TaskStatus.RUNNING);

                        try {
                                Thread.sleep(500);
                        } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();

                                task.setStatus(TaskStatus.FAILED);

                                return TaskExecutionResult.FAILED;
                        }

                        task.setStatus(TaskStatus.SUCCESS);

                        return TaskExecutionResult.SUCCESS;
                };

                TaskExecutionPool pool = new TaskExecutionPool(2, executor);

                pool.submit(
                                new TaskExecutionRequest(
                                                taskA.getId(),
                                                taskA.getCommand(),
                                                taskA.getRetryCount()));

                pool.submit(
                                new TaskExecutionRequest(
                                                taskB.getId(),
                                                taskB.getCommand(),
                                                taskB.getRetryCount()));

                Future<TaskExecution> first = pool.takeCompleted();

                Future<TaskExecution> second = pool.takeCompleted();

                TaskExecution firstExecution = first.get();

                TaskExecution secondExecution = second.get();

                assertEquals(
                                TaskExecutionResult.SUCCESS,
                                firstExecution.result());

                assertEquals(
                                TaskExecutionResult.SUCCESS,
                                secondExecution.result());

                assertEquals(
                                Set.of("A", "B"),
                                Set.of(
                                                firstExecution.taskId(),
                                                secondExecution.taskId()));

                assertTrue(firstExecution.startedAt() > 0);
                assertTrue(firstExecution.completedAt() >= firstExecution.startedAt());
                assertTrue(firstExecution.durationMillis() >= 0);

                pool.shutdown();
        }

        @Test
        void shouldConvertTaskExceptionIntoFailure()
                        throws Exception {

                Task task = new Task(
                                "A",
                                "Task A",
                                "explode",
                                Set.of());

                TaskExecutor executor = ignored -> {
                        throw new RuntimeException("Something went wrong");
                };

                TaskExecutionPool pool = new TaskExecutionPool(1, executor);

                try {

                        pool.submit(
                                        new TaskExecutionRequest(
                                                        task.getId(),
                                                        task.getCommand(),
                                                        task.getRetryCount()));

                        Future<TaskExecution> future = pool.takeCompleted();

                        TaskExecution execution = future.get();

                        assertEquals(
                                        "A",
                                        execution.taskId());

                        assertEquals(
                                        TaskExecutionResult.FAILED,
                                        execution.result());

                } finally {

                        pool.shutdown();
                }
        }

        @Test
        void shouldExecuteRealCommandsThroughProcessExecutor()
                        throws Exception {

                TaskExecutionPool pool = new TaskExecutionPool(
                                2,
                                new ProcessTaskExecutor());

                try {
                        pool.submit(
                                        new TaskExecutionRequest(
                                                        "A",
                                                        "echo Orchestra",
                                                        0));

                        Future<TaskExecution> future = pool.takeCompleted();

                        TaskExecution execution = future.get();

                        assertEquals(
                                        "A",
                                        execution.taskId());

                        assertEquals(
                                        TaskExecutionResult.SUCCESS,
                                        execution.result());

                        assertTrue(
                                        execution.completedAt() >= execution.startedAt());

                } finally {
                        pool.shutdown();
                }
        }

        @Test
        void shouldReportRealCommandFailureThroughProcessExecutor()
                        throws Exception {

                TaskExecutionPool pool = new TaskExecutionPool(
                                1,
                                new ProcessTaskExecutor());

                try {
                        pool.submit(
                                        new TaskExecutionRequest(
                                                        "A",
                                                        "exit 1",
                                                        0));

                        Future<TaskExecution> future = pool.takeCompleted();

                        TaskExecution execution = future.get();

                        assertEquals(
                                        "A",
                                        execution.taskId());

                        assertEquals(
                                        TaskExecutionResult.FAILED,
                                        execution.result());

                        assertTrue(
                                        execution.completedAt() >= execution.startedAt());

                } finally {
                        pool.shutdown();
                }
        }

        @Test
        void shouldExecuteMultipleRealCommandsConcurrently()
                        throws Exception {

                TaskExecutionPool pool = new TaskExecutionPool(
                                2,
                                new ProcessTaskExecutor());

                try {
                        long start = System.currentTimeMillis();

                        pool.submit(
                                        new TaskExecutionRequest(
                                                        "A",
                                                        "ping 127.0.0.1 -n 2 > nul",
                                                        0));

                        pool.submit(
                                        new TaskExecutionRequest(
                                                        "B",
                                                        "ping 127.0.0.1 -n 2 > nul",
                                                        0));

                        Future<TaskExecution> first = pool.takeCompleted();

                        Future<TaskExecution> second = pool.takeCompleted();

                        TaskExecution executionA = first.get();
                        TaskExecution executionB = second.get();

                        long elapsed = System.currentTimeMillis() - start;

                        assertEquals(
                                        TaskExecutionResult.SUCCESS,
                                        executionA.result());

                        assertEquals(
                                        TaskExecutionResult.SUCCESS,
                                        executionB.result());

                        assertEquals(
                                        2,
                                        java.util.Set.of(
                                                        executionA.taskId(),
                                                        executionB.taskId()).size());

                        assertTrue(
                                        executionA.durationMillis() > 0);

                        assertTrue(
                                        executionB.durationMillis() > 0);

                        assertTrue(
                                        elapsed < 1800,
                                        "Commands did not execute concurrently. Elapsed: "
                                                        + elapsed + " ms");

                } finally {
                        pool.shutdown();
                }
        }
}