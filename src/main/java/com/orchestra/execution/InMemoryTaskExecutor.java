package com.orchestra.execution;

import com.orchestra.workflow.Task;
import com.orchestra.workflow.TaskStatus;

public class InMemoryTaskExecutor implements TaskExecutor {

    @Override
    public TaskExecutionResult execute(Task task) {

        task.setStatus(TaskStatus.RUNNING);

        System.out.println(
                "Executing task: " + task.getId()
        );

        task.setStatus(TaskStatus.SUCCESS);

        System.out.println(
                "Task completed: " + task.getId()
        );

        return TaskExecutionResult.SUCCESS;
    }
}