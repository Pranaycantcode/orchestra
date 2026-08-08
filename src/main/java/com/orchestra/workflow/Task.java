package com.orchestra.workflow;

import java.util.Set;

public class Task {

    private final String id;
    private final String name;
    private final String command;
    private final Set<String> dependencies;

    private TaskStatus status;

    public Task(
            String id,
            String name,
            String command,
            Set<String> dependencies
    ) {
        this.id = id;
        this.name = name;
        this.command = command;
        this.dependencies = dependencies;
        this.status = TaskStatus.PENDING;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getCommand() {
        return command;
    }

    public Set<String> getDependencies() {
        return dependencies;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }
}