package com.orchestra.scheduler;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.orchestra.workflow.DagValidator;
import com.orchestra.workflow.Task;
import com.orchestra.workflow.TaskStatus;
import com.orchestra.workflow.Workflow;

public class WorkflowScheduler {

    private final DagValidator dagValidator;

    public WorkflowScheduler(DagValidator dagValidator) {
        this.dagValidator = dagValidator;
    }

    public void execute(Workflow workflow) {

        dagValidator.validate(workflow);

        Map<String, Task> tasks = workflow.getTasks();

        Map<String, Integer> remainingDependencies =
                buildDependencyCounts(tasks);

        Map<String, List<String>> dependents =
                buildDependentsMap(tasks);

        Queue<String> readyQueue =
                initializeReadyQueue(remainingDependencies);

        while (!readyQueue.isEmpty()) {

            String taskId = readyQueue.poll();

            Task task = tasks.get(taskId);

            executeTask(task);

            for (String dependentId : dependents.get(taskId)) {

                int remaining =
                        remainingDependencies.get(dependentId) - 1;

                remainingDependencies.put(
                        dependentId,
                        remaining
                );

                if (remaining == 0) {
                    readyQueue.add(dependentId);
                }
            }
        }
    }

    private Map<String, Integer> buildDependencyCounts(
            Map<String, Task> tasks
    ) {

        Map<String, Integer> counts = new HashMap<>();

        for (Task task : tasks.values()) {

            counts.put(
                    task.getId(),
                    task.getDependencies().size()
            );
        }

        return counts;
    }

    private Map<String, List<String>> buildDependentsMap(
            Map<String, Task> tasks
    ) {

        Map<String, List<String>> dependents = new HashMap<>();

        for (String taskId : tasks.keySet()) {
            dependents.put(taskId, new ArrayList<>());
        }

        for (Task task : tasks.values()) {

            for (String dependencyId : task.getDependencies()) {

                dependents
                        .get(dependencyId)
                        .add(task.getId());
            }
        }

        return dependents;
    }

    private Queue<String> initializeReadyQueue(
            Map<String, Integer> remainingDependencies
    ) {

        Queue<String> readyQueue = new ArrayDeque<>();

        for (Map.Entry<String, Integer> entry :
                remainingDependencies.entrySet()) {

            if (entry.getValue() == 0) {
                readyQueue.add(entry.getKey());
            }
        }

        return readyQueue;
    }

    private void executeTask(Task task) {

        task.setStatus(TaskStatus.RUNNING);

        System.out.println(
                "Executing task: " + task.getId()
        );

        task.setStatus(TaskStatus.SUCCESS);

        System.out.println(
                "Task completed: " + task.getId()
        );
    }
}