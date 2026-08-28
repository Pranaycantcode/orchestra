package com.orchestra.execution;

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
    public void submit(Task task) {

        completionService.submit(
                () -> {

                    try {

                        TaskExecutionResult result = taskExecutor.execute(task);

                        return new TaskExecution(
                                task.getId(),
                                result);

                    } catch (Exception e) {

                        return new TaskExecution(
                                task.getId(),
                                TaskExecutionResult.FAILED);
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