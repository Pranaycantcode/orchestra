package com.orchestra.scheduler;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.orchestra.execution.TaskExecution;
import com.orchestra.execution.TaskExecutionPool;
import com.orchestra.execution.TaskExecutionResult;
import com.orchestra.execution.TaskExecutor;
import com.orchestra.workflow.DagValidator;
import com.orchestra.workflow.Task;
import com.orchestra.workflow.TaskStatus;
import com.orchestra.workflow.Workflow;

public class WorkflowScheduler {

    private final DagValidator dagValidator;
    private final TaskExecutor taskExecutor;
    private final TaskExecutionPool executionPool;

    public WorkflowScheduler(
            DagValidator dagValidator,
            TaskExecutor taskExecutor,
            TaskExecutionPool executionPool) {
        this.dagValidator = dagValidator;
        this.taskExecutor = taskExecutor;
        this.executionPool = executionPool;
    }

    public void execute(Workflow workflow) {

        dagValidator.validate(workflow);

        Map<String, Task> tasks = workflow.getTasks();

        Map<String, Integer> remainingDependencies = buildDependencyCounts(tasks);

        Map<String, List<String>> dependents = buildDependentsMap(tasks);

        Queue<String> readyQueue = initializeReadyQueue(
                remainingDependencies);

        Set<String> blockedTasks = new HashSet<>();

        int runningTasks = 0;

        while (!readyQueue.isEmpty()
                || runningTasks > 0) {

            while (!readyQueue.isEmpty()) {

                String taskId = readyQueue.poll();

                Task task = tasks.get(taskId);

                executionPool.submit(
                        task,
                        taskExecutor);

                runningTasks++;
            }

            if (runningTasks > 0) {

                try {

                    Future<TaskExecution> completed = executionPool.takeCompleted();

                    TaskExecution execution = completed.get();

                    runningTasks--;

                    handleTaskCompletion(
                            execution,
                            tasks,
                            dependents,
                            remainingDependencies,
                            blockedTasks,
                            readyQueue);

                } catch (InterruptedException e) {

                    Thread.currentThread().interrupt();

                    throw new RuntimeException(
                            "Scheduler interrupted",
                            e);

                } catch (ExecutionException e) {

                    throw new RuntimeException(
                            "Task execution failed unexpectedly",
                            e);
                }
            }
        }
    }

    private void handleTaskCompletion(
            TaskExecution execution,
            Map<String, Task> tasks,
            Map<String, List<String>> dependents,
            Map<String, Integer> remainingDependencies,
            Set<String> blockedTasks,
            Queue<String> readyQueue) {

        String taskId = execution.taskId();

        TaskExecutionResult result = execution.result();

        if (result == TaskExecutionResult.SUCCESS) {

            handleSuccess(
                    taskId,
                    dependents,
                    remainingDependencies,
                    blockedTasks,
                    readyQueue);

        } else {

            Task task = tasks.get(taskId);

            if (task.canRetry()) {

                task.setRetryCount(
                        task.getRetryCount() + 1);

                task.setStatus(
                        TaskStatus.PENDING);

                readyQueue.add(taskId);

            } else {

                handleFailure(
                        taskId,
                        dependents,
                        tasks,
                        blockedTasks);
            }
        }
    }

    private void handleSuccess(
            String taskId,
            Map<String, List<String>> dependents,
            Map<String, Integer> remainingDependencies,
            Set<String> blockedTasks,
            Queue<String> readyQueue) {

        for (String dependentId : dependents.get(taskId)) {

            int remaining = remainingDependencies.get(dependentId) - 1;

            remainingDependencies.put(
                    dependentId,
                    remaining);

            if (remaining == 0
                    && !blockedTasks.contains(dependentId)) {

                readyQueue.add(dependentId);
            }
        }
    }

    private void handleFailure(
            String taskId,
            Map<String, List<String>> dependents,
            Map<String, Task> tasks,
            Set<String> blockedTasks) {

        for (String dependentId : dependents.get(taskId)) {

            if (blockedTasks.contains(dependentId)) {
                continue;
            }

            blockedTasks.add(dependentId);

            tasks.get(dependentId)
                    .setStatus(TaskStatus.BLOCKED);

            handleFailure(
                    dependentId,
                    dependents,
                    tasks,
                    blockedTasks);
        }
    }

    private Map<String, Integer> buildDependencyCounts(
            Map<String, Task> tasks) {

        Map<String, Integer> counts = new HashMap<>();

        for (Task task : tasks.values()) {

            counts.put(
                    task.getId(),
                    task.getDependencies().size());
        }

        return counts;
    }

    private Map<String, List<String>> buildDependentsMap(
            Map<String, Task> tasks) {

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
            Map<String, Integer> remainingDependencies) {

        Queue<String> readyQueue = new ArrayDeque<>();

        for (Map.Entry<String, Integer> entry : remainingDependencies.entrySet()) {

            if (entry.getValue() == 0) {
                readyQueue.add(entry.getKey());
            }
        }

        return readyQueue;
    }

    private void executeTask(Task task) {

        task.setStatus(TaskStatus.RUNNING);

        System.out.println(
                "Executing task: " + task.getId());

        task.setStatus(TaskStatus.SUCCESS);

        System.out.println(
                "Task completed: " + task.getId());
    }
}