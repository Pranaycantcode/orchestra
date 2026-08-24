package com.orchestra.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;

import org.junit.jupiter.api.Test;

import com.orchestra.workflow.Task;
import com.orchestra.workflow.TaskStateManager;
import com.orchestra.workflow.TaskStatus;

class InMemoryTaskExecutorTest {

    @Test
    void shouldExecuteTaskSuccessfully() {

        Task task = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of()
        );

        InMemoryTaskExecutor executor =
                new InMemoryTaskExecutor(
                        new TaskStateManager()
                );

        TaskExecutionResult result =
                executor.execute(task);

        assertEquals(
                TaskExecutionResult.SUCCESS,
                result
        );

        assertEquals(
                TaskStatus.SUCCESS,
                task.getStatus()
        );
    }
}