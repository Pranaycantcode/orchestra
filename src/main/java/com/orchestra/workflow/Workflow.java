package com.orchestra.workflow;

import java.util.Map;

public class Workflow {

    private final String id;
    private final String name;
    private final Map<String, Task> tasks;

    public Workflow(
            String id,
            String name,
            Map<String, Task> tasks
    ) {
        this.id = id;
        this.name = name;
        this.tasks = tasks;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Map<String, Task> getTasks() {
        return tasks;
    }
}