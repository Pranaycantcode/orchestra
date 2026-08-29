package com.orchestra.execution;

public record TaskExecution(

        String taskId,

        TaskExecutionResult result,

        long startedAt,

        long completedAt

) {

    public long durationMillis() {
        return completedAt - startedAt;
    }
}