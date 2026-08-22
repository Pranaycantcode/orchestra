package com.orchestra.workflow;

public class TaskStateManager {

    public void markRunning(Task task) {
        transition(task, TaskStatus.RUNNING);
    }

    public void markPending(Task task) {
        transition(task, TaskStatus.PENDING);
    }

    public void markSuccess(Task task) {
        transition(task, TaskStatus.SUCCESS);
    }

    public void markFailed(Task task) {
        transition(task, TaskStatus.FAILED);
    }

    public void markBlocked(Task task) {
        transition(task, TaskStatus.BLOCKED);
    }

    private void transition(
            Task task,
            TaskStatus targetStatus) {

        TaskStatus currentStatus =
                task.getStatus();

        if (!isValidTransition(
                currentStatus,
                targetStatus)) {

            throw new IllegalStateException(
                    "Invalid task state transition: "
                            + currentStatus
                            + " -> "
                            + targetStatus
            );
        }

        task.setStatus(targetStatus);
    }

    private boolean isValidTransition(
            TaskStatus current,
            TaskStatus target) {

        return switch (current) {

            case PENDING ->
                    target == TaskStatus.RUNNING;

            case RUNNING ->
                    target == TaskStatus.SUCCESS
                            || target == TaskStatus.FAILED
                            || target == TaskStatus.BLOCKED;

            case FAILED ->
                    target == TaskStatus.PENDING;

            case SUCCESS, BLOCKED ->
                    false;
        };
    }
}