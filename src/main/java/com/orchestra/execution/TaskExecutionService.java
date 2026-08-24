package com.orchestra.execution;

import java.util.concurrent.Future;

import com.orchestra.workflow.Task;

public interface TaskExecutionService {

    void submit(
            Task task,
            TaskExecutor taskExecutor);

    Future<TaskExecution> takeCompleted()
            throws InterruptedException;
}