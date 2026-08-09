package com.orchestra.execution;

import com.orchestra.workflow.Task;

public interface TaskExecutor {

    TaskExecutionResult execute(Task task);
}