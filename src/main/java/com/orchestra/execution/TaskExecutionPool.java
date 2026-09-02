package com.orchestra.execution;

import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.orchestra.workflow.Task;

public class TaskExecutionPool
        implements TaskExecutionService {

    private final ExecutorService executorService;

    private final CompletionService<TaskExecution> completionService;

    private final TaskExecutor taskExecutor;

    public TaskExecutionPool(
            int threadCount,
            TaskExecutor taskExecutor) {

        this.executorService = Executors.newFixedThreadPool(threadCount);

        this.completionService = new ExecutorCompletionService<>(
                executorService);

        this.taskExecutor = taskExecutor;
    }

    @Override
    public void submit(TaskExecutionRequest request) {

        completionService.submit(
                () -> {

                    long startedAt = System.currentTimeMillis();

                    try {

                        Task task = new Task(
                                request.taskId(),
                                request.taskId(),
                                request.command(),
                                Set.of());

                        task.setRetryCount(request.retryCount());

                        TaskExecutionResult result = taskExecutor.execute(task);

                        long completedAt = System.currentTimeMillis();

                        return new TaskExecution(
                                task.getId(),
                                result,
                                startedAt,
                                completedAt);

                    } catch (Exception e) {

                        long completedAt = System.currentTimeMillis();

                        return new TaskExecution(
                                request.taskId(),
                                TaskExecutionResult.FAILED,
                                startedAt,
                                completedAt);
                    }
                });
    }

    public Future<TaskExecution> takeCompleted()
            throws InterruptedException {

        return completionService.take();
    }

    public void shutdown() {
        executorService.shutdown();
    }
}