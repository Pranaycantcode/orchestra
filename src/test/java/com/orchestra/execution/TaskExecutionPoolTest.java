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
}