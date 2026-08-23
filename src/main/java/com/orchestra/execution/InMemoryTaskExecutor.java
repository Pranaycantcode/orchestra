package com.orchestra.execution;

import com.orchestra.workflow.Task;
import com.orchestra.workflow.TaskStateManager;

public class InMemoryTaskExecutor implements TaskExecutor {

    private final TaskStateManager stateManager;

    public InMemoryTaskExecutor(TaskStateManager stateManager) {
        this.stateManager = stateManager;
    }

    @Override
    public TaskExecutionResult execute(Task task) {

        stateManager.markRunning(task);

        System.out.println(
                "Executing task: " + task.getId()
        );

        stateManager.markSuccess(task);

        System.out.println(
                "Task completed: " + task.getId()
        );

        return TaskExecutionResult.SUCCESS;
    }
}