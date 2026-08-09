package com.orchestra.execution;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.orchestra.workflow.Task;

public class TaskExecutionPool {

    private final ExecutorService executorService;

    private final CompletionService<TaskExecution> completionService;

    public TaskExecutionPool(int threadCount) {

        this.executorService = Executors.newFixedThreadPool(threadCount);

        this.completionService = new ExecutorCompletionService<>(
                executorService);
    }

    public void submit(
            Task task,
            TaskExecutor taskExecutor) {

        completionService.submit(
                () -> new TaskExecution(
                        task.getId(),
                        taskExecutor.execute(task)));
    }

    public Future<TaskExecution> takeCompleted()
            throws InterruptedException {

        return completionService.take();
    }

    public void shutdown() {
        executorService.shutdown();
    }
}