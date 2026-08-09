package com.orchestra.execution;

public record TaskExecution(
        String taskId,
        TaskExecutionResult result
) {
}