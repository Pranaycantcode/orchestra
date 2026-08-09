package com.orchestra.execution;

import java.util.Set;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
                Set.of()
        );

        Task taskB = new Task(
                "B",
                "Task B",
                "sleep",
                Set.of()
        );

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

        TaskExecutionPool pool =
                new TaskExecutionPool(2);

        pool.submit(taskA, executor);
        pool.submit(taskB, executor);

        Future<TaskExecution> first =
                pool.takeCompleted();

        Future<TaskExecution> second =
                pool.takeCompleted();

        TaskExecution firstExecution =
                first.get();

        TaskExecution secondExecution =
                second.get();

        assertEquals(
                TaskExecutionResult.SUCCESS,
                firstExecution.result()
        );

        assertEquals(
                TaskExecutionResult.SUCCESS,
                secondExecution.result()
        );

        assertEquals(
                Set.of("A", "B"),
                Set.of(
                        firstExecution.taskId(),
                        secondExecution.taskId()
                )
        );

        assertEquals(
                TaskStatus.SUCCESS,
                taskA.getStatus()
        );

        assertEquals(
                TaskStatus.SUCCESS,
                taskB.getStatus()
        );

        pool.shutdown();
    }
}