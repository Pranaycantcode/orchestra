package com.orchestra.workflow;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class DagValidatorTest {

    @Test
    void shouldAcceptValidWorkflow() {

        Task taskA = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of()
        );

        Task taskB = new Task(
                "B",
                "Task B",
                "echo B",
                Set.of("A")
        );

        Task taskC = new Task(
                "C",
                "Task C",
                "echo C",
                Set.of("B")
        );

        Workflow workflow = new Workflow(
                "workflow-1",
                "Simple workflow",
                Map.of(
                        "A", taskA,
                        "B", taskB,
                        "C", taskC
                )
        );

        DagValidator validator = new DagValidator();

        assertDoesNotThrow(() -> validator.validate(workflow));
    }

    @Test
    void shouldRejectUnknownDependency() {

        Task taskA = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of("DOES_NOT_EXIST")
        );

        Workflow workflow = new Workflow(
                "workflow-2",
                "Invalid dependency workflow",
                Map.of(
                        "A", taskA
                )
        );

        DagValidator validator = new DagValidator();

        assertThrows(
                WorkflowValidationException.class,
                () -> validator.validate(workflow)
        );
    }

    @Test
    void shouldRejectCyclicWorkflow() {

        Task taskA = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of("B")
        );

        Task taskB = new Task(
                "B",
                "Task B",
                "echo B",
                Set.of("A")
        );

        Workflow workflow = new Workflow(
                "workflow-3",
                "Cyclic workflow",
                Map.of(
                        "A", taskA,
                        "B", taskB
                )
        );

        DagValidator validator = new DagValidator();

        assertThrows(
                WorkflowValidationException.class,
                () -> validator.validate(workflow)
        );
    }

    @Test
    void shouldAcceptWorkflowWithParallelBranches() {

        Task taskA = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of()
        );

        Task taskB = new Task(
                "B",
                "Task B",
                "echo B",
                Set.of("A")
        );

        Task taskC = new Task(
                "C",
                "Task C",
                "echo C",
                Set.of("A")
        );

        Task taskD = new Task(
                "D",
                "Task D",
                "echo D",
                Set.of("B", "C")
        );

        Workflow workflow = new Workflow(
                "workflow-4",
                "Parallel workflow",
                Map.of(
                        "A", taskA,
                        "B", taskB,
                        "C", taskC,
                        "D", taskD
                )
        );

        DagValidator validator = new DagValidator();

        assertDoesNotThrow(() -> validator.validate(workflow));
    }

    @Test
    void shouldAcceptIndependentTasks() {

        Task taskA = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of()
        );

        Task taskB = new Task(
                "B",
                "Task B",
                "echo B",
                Set.of()
        );

        Task taskC = new Task(
                "C",
                "Task C",
                "echo C",
                Set.of()
        );

        Workflow workflow = new Workflow(
                "workflow-5",
                "Independent tasks",
                Map.of(
                        "A", taskA,
                        "B", taskB,
                        "C", taskC
                )
        );

        DagValidator validator = new DagValidator();

        assertDoesNotThrow(() -> validator.validate(workflow));
    }

    @Test
    void shouldRejectLargerCycle() {

        Task taskA = new Task(
                "A",
                "Task A",
                "echo A",
                Set.of()
        );

        Task taskB = new Task(
                "B",
                "Task B",
                "echo B",
                Set.of("A", "D")
        );

        Task taskC = new Task(
                "C",
                "Task C",
                "echo C",
                Set.of("B")
        );

        Task taskD = new Task(
                "D",
                "Task D",
                "echo D",
                Set.of("C")
        );

        Workflow workflow = new Workflow(
                "workflow-6",
                "Larger cyclic workflow",
                Map.of(
                        "A", taskA,
                        "B", taskB,
                        "C", taskC,
                        "D", taskD
                )
        );

        DagValidator validator = new DagValidator();

        assertThrows(
                WorkflowValidationException.class,
                () -> validator.validate(workflow)
        );
    }
}