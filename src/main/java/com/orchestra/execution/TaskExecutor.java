package com.orchestra.execution;

import com.orchestra.workflow.Task;

public interface TaskExecutor {

    void execute(Task task);
}