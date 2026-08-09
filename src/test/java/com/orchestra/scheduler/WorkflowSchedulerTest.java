package com.orchestra.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import com.orchestra.execution.TaskExecutionResult;
import com.orchestra.execution.TaskExecutor;
import com.orchestra.workflow.DagValidator;
import com.orchestra.workflow.Task;
import com.orchestra.workflow.TaskStatus;
import com.orchestra.workflow.Workflow;
import com.orchestra.workflow.WorkflowValidationException;

class WorkflowSchedulerTest {

    private static class RecordingTaskExecutor implements TaskExecutor {

        private final List<String> executionOrder = new ArrayList<>();

        @Override
        public TaskExecutionResult execute(Task task) {

            executionOrder.add(task.getId());

            task.setStatus(TaskStatus.RUNNING);
            task.setStatus(TaskStatus.SUCCESS);

            return TaskExecutionResult.SUCCESS;
        }

        public List<String> getExecutionOrder() {
            return executionOrder;
        }
    }

    private static class ControlledTaskExecutor
            implements TaskExecutor {

        private final Set<String> tasksToFail;
        private final List<String> executionOrder = new ArrayList<>();

        ControlledTaskExecutor(Set<String> tasksToFail) {
            this.tasksToFail = tasksToFail;
        }

        @Override
        public TaskExecutionResult execute(Task task) {

            executionOrder.add(task.getId());

            task.setStatus(TaskStatus.RUNNING);

            if (tasksToFail.contains(task.getId())) {

                task.setStatus(TaskStatus.FAILED);

                return TaskExecutionResult.FAILED;
            }

            task.setStatus(TaskStatus.SUCCESS);

            return TaskExecutionResult.SUCCESS;
        }

        public List<String> getExecutionOrder() {
            return executionOrder;
        }
    }

    @Test
    void shouldExecuteTasksInDependencyOrder() {

        Task taskA = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of());

        Task taskB = new Task(
                "B",
                "Task B",
                "echo B",
                Set.of("A"));

        Task taskC = new Task(
                "C",
                "Task C",
                "echo C",
                Set.of("B"));

        Workflow workflow = new Workflow(
                "workflow-1",
                "Simple chain",
                Map.of(
                        "A", taskA,
                        "B", taskB,
                        "C", taskC));

        RecordingTaskExecutor executor = new RecordingTaskExecutor();

        WorkflowScheduler scheduler = new WorkflowScheduler(
                new DagValidator(),
                executor);

        scheduler.execute(workflow);

        assertEquals(
                List.of("A", "B", "C"),
                executor.getExecutionOrder());

        assertEquals(TaskStatus.SUCCESS, taskA.getStatus());
        assertEquals(TaskStatus.SUCCESS, taskB.getStatus());
        assertEquals(TaskStatus.SUCCESS, taskC.getStatus());
    }

    @Test
    void shouldRespectDependenciesWithParallelBranches() {

        Task taskA = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of());

        Task taskB = new Task(
                "B",
                "Task B",
                "echo B",
                Set.of("A"));

        Task taskC = new Task(
                "C",
                "Task C",
                "echo C",
                Set.of("A"));

        Task taskD = new Task(
                "D",
                "Task D",
                "echo D",
                Set.of("B", "C"));

        Workflow workflow = new Workflow(
                "workflow-2",
                "Parallel branches",
                Map.of(
                        "A", taskA,
                        "B", taskB,
                        "C", taskC,
                        "D", taskD));

        RecordingTaskExecutor executor = new RecordingTaskExecutor();

        WorkflowScheduler scheduler = new WorkflowScheduler(
                new DagValidator(),
                executor);

        scheduler.execute(workflow);

        List<String> order = executor.getExecutionOrder();

        int indexA = order.indexOf("A");
        int indexB = order.indexOf("B");
        int indexC = order.indexOf("C");
        int indexD = order.indexOf("D");

        assertTrue(indexA < indexB);
        assertTrue(indexA < indexC);

        assertTrue(indexB < indexD);
        assertTrue(indexC < indexD);
    }

    @Test
    void shouldExecuteIndependentTasks() {

        Task taskA = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of());

        Task taskB = new Task(
                "B",
                "Task B",
                "echo B",
                Set.of());

        Task taskC = new Task(
                "C",
                "Task C",
                "echo C",
                Set.of());

        Workflow workflow = new Workflow(
                "workflow-3",
                "Independent tasks",
                Map.of(
                        "A", taskA,
                        "B", taskB,
                        "C", taskC));

        RecordingTaskExecutor executor = new RecordingTaskExecutor();

        WorkflowScheduler scheduler = new WorkflowScheduler(
                new DagValidator(),
                executor);

        scheduler.execute(workflow);

        assertEquals(
                3,
                executor.getExecutionOrder().size());

        assertTrue(
                executor.getExecutionOrder()
                        .containsAll(List.of("A", "B", "C")));
    }

    @Test
    void shouldNotExecuteInvalidWorkflow() {

        Task taskA = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of("B"));

        Task taskB = new Task(
                "B",
                "Task B",
                "echo B",
                Set.of("A"));

        Workflow workflow = new Workflow(
                "workflow-4",
                "Cyclic workflow",
                Map.of(
                        "A", taskA,
                        "B", taskB));

        RecordingTaskExecutor executor = new RecordingTaskExecutor();

        WorkflowScheduler scheduler = new WorkflowScheduler(
                new DagValidator(),
                executor);

        assertThrows(
                WorkflowValidationException.class,
                () -> scheduler.execute(workflow));

        assertTrue(
                executor.getExecutionOrder().isEmpty());
    }

    @Test
    void shouldBlockDependentTaskWhenDependencyFails() {

        Task taskA = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of());

        Task taskB = new Task(
                "B",
                "Task B",
                "echo B",
                Set.of("A"));

        Task taskC = new Task(
                "C",
                "Task C",
                "echo C",
                Set.of("B"));

        Workflow workflow = new Workflow(
                "workflow-failure-1",
                "Failure propagation",
                Map.of(
                        "A", taskA,
                        "B", taskB,
                        "C", taskC));

        ControlledTaskExecutor executor = new ControlledTaskExecutor(Set.of("B"));

        WorkflowScheduler scheduler = new WorkflowScheduler(
                new DagValidator(),
                executor);

        scheduler.execute(workflow);

        assertEquals(
                TaskStatus.SUCCESS,
                taskA.getStatus());

        assertEquals(
                TaskStatus.FAILED,
                taskB.getStatus());

        assertEquals(
                TaskStatus.BLOCKED,
                taskC.getStatus());

        assertEquals(
                List.of("A", "B"),
                executor.getExecutionOrder());
    }
}