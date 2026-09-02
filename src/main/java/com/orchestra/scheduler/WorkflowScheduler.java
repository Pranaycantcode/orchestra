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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.orchestra.execution.RetryScheduler;
import com.orchestra.execution.TaskExecution;
import com.orchestra.execution.TaskExecutionPool;
import com.orchestra.execution.TaskExecutionRequest;
import com.orchestra.execution.TaskExecutionResult;
import com.orchestra.workflow.DagValidator;
import com.orchestra.workflow.RetryPolicy;
import com.orchestra.workflow.Task;
import com.orchestra.workflow.TaskStateManager;
import com.orchestra.workflow.Workflow;

public class WorkflowScheduler {

    private final DagValidator dagValidator;
    private final TaskExecutionPool executionPool;
    private final RetryPolicy retryPolicy;
    private final RetryScheduler retryScheduler;
    private final AtomicInteger pendingRetries = new AtomicInteger(0);
    private final Object retryMonitor = new Object();
    private final TaskStateManager stateManager;

    public WorkflowScheduler(
            DagValidator dagValidator,
            TaskExecutionPool executionPool) {

        this(
                dagValidator,
                executionPool,
                new RetryPolicy(100),
                new RetryScheduler(
                        Executors.newScheduledThreadPool(1)),
                new TaskStateManager());
    }

    public WorkflowScheduler(
            DagValidator dagValidator,
            TaskExecutionPool executionPool,
            RetryPolicy retryPolicy,
            RetryScheduler retryScheduler) {

        this(
                dagValidator,
                executionPool,
                retryPolicy,
                retryScheduler,
                new TaskStateManager());
    }

    public WorkflowScheduler(
            DagValidator dagValidator,
            TaskExecutionPool executionPool,
            RetryPolicy retryPolicy,
            RetryScheduler retryScheduler,
            TaskStateManager stateManager) {

        this.dagValidator = dagValidator;
        this.executionPool = executionPool;
        this.retryPolicy = retryPolicy;
        this.retryScheduler = retryScheduler;
        this.stateManager = stateManager;
    }

    public void execute(Workflow workflow) {

        dagValidator.validate(workflow);

        Map<String, Task> tasks = workflow.getTasks();

        Map<String, Integer> remainingDependencies = buildDependencyCounts(tasks);

        Map<String, List<String>> dependents = buildDependentsMap(tasks);

        Queue<String> readyQueue = initializeReadyQueue(remainingDependencies);

        Set<String> blockedTasks = new HashSet<>();

        int runningTasks = 0;

        while (!readyQueue.isEmpty()
                || runningTasks > 0
                || pendingRetries.get() > 0) {

            while (!readyQueue.isEmpty()) {

                String taskId = readyQueue.poll();

                Task task = tasks.get(taskId);

                stateManager.markRunning(task);

                executionPool.submit(
                        new TaskExecutionRequest(
                                task.getId(),
                                task.getCommand(),
                                task.getRetryCount()));

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

            } else if (pendingRetries.get() > 0) {

                synchronized (retryMonitor) {

                    if (pendingRetries.get() > 0
                            && readyQueue.isEmpty()) {

                        try {

                            retryMonitor.wait();

                        } catch (InterruptedException e) {

                            Thread.currentThread().interrupt();

                            throw new RuntimeException(
                                    "Scheduler interrupted",
                                    e);
                        }
                    }
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

        Task task = tasks.get(taskId);

        if (result == TaskExecutionResult.SUCCESS) {

            stateManager.markSuccess(task);

            handleSuccess(
                    taskId,
                    dependents,
                    remainingDependencies,
                    blockedTasks,
                    readyQueue);

        } else {

            stateManager.markFailed(task);

            if (task.canRetry()) {

                task.setRetryCount(
                        task.getRetryCount() + 1);

                stateManager.markPending(task);

                pendingRetries.incrementAndGet();

                long delay = retryPolicy.getDelayMillis(
                        task.getRetryCount());

                retryScheduler.schedule(
                        () -> {

                            readyQueue.add(taskId);

                            pendingRetries.decrementAndGet();

                            synchronized (retryMonitor) {
                                retryMonitor.notify();
                            }

                        },
                        delay);

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

            stateManager.markBlocked(
                    tasks.get(dependentId));

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
            dependents.put(
                    taskId,
                    new ArrayList<>());
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
}