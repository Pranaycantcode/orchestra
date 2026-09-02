package com.orchestra.execution;

import java.io.IOException;

import com.orchestra.workflow.Task;

public class ProcessTaskExecutor implements TaskExecutor {

    @Override
    public TaskExecutionResult execute(Task task) {

        try {
            Process process = new ProcessBuilder(
                    "cmd.exe",
                    "/c",
                    task.getCommand()
            ).inheritIO().start();

            int exitCode = process.waitFor();

            if (exitCode == 0) {
                return TaskExecutionResult.SUCCESS;
            }

            return TaskExecutionResult.FAILED;

        } catch (IOException | InterruptedException e) {

            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            return TaskExecutionResult.FAILED;
        }
    }
}