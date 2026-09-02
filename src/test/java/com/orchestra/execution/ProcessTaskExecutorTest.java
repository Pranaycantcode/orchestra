package com.orchestra.execution;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import com.orchestra.workflow.Task;

class ProcessTaskExecutorTest {

    @Test
    void shouldExecuteSuccessfulCommand() {

        Task task = new Task(
                "A",
                "Task A",
                "echo Hello Orchestra",
                Set.of()
        );

        ProcessTaskExecutor executor = new ProcessTaskExecutor();

        TaskExecutionResult result = executor.execute(task);

        assertEquals(
                TaskExecutionResult.SUCCESS,
                result
        );
    }

    @Test
    void shouldReturnFailureForFailedCommand() {

        Task task = new Task(
                "A",
                "Task A",
                "exit 1",
                Set.of()
        );

        ProcessTaskExecutor executor = new ProcessTaskExecutor();

        TaskExecutionResult result = executor.execute(task);

        assertEquals(
                TaskExecutionResult.FAILED,
                result
        );
    }
}