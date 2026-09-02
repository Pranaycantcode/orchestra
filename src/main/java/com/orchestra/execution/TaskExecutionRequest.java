package com.orchestra.execution;

public record TaskExecutionRequest(

        String taskId,
        String command,
        int retryCount

) {
}