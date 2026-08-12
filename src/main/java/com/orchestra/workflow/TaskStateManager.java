package com.orchestra.workflow;

public class TaskStateManager {

    public void markRunning(Task task) {
        task.setStatus(TaskStatus.RUNNING);
    }

    public void markSuccess(Task task) {
        task.setStatus(TaskStatus.SUCCESS);
    }

    public void markFailed(Task task) {
        task.setStatus(TaskStatus.FAILED);
    }

    public void markBlocked(Task task) {
        task.setStatus(TaskStatus.BLOCKED);
    }
}