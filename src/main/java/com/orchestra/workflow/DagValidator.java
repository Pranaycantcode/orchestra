package com.orchestra.workflow;

import java.util.HashMap;
import java.util.Map;

public class DagValidator {

    public void validate(Workflow workflow) {

        validateDependenciesExist(workflow);
        validateNoCycles(workflow);
    }

    private void validateDependenciesExist(Workflow workflow) {

        Map<String, Task> tasks = workflow.getTasks();

        for (Task task : tasks.values()) {

            for (String dependencyId : task.getDependencies()) {

                if (!tasks.containsKey(dependencyId)) {

                    throw new WorkflowValidationException(
                            "Task '" + task.getId()
                                    + "' depends on unknown task '"
                                    + dependencyId + "'"
                    );
                }
            }
        }
    }

    private void validateNoCycles(Workflow workflow) {

        Map<String, Task> tasks = workflow.getTasks();

        Map<String, VisitState> visitStates = new HashMap<>();

        for (String taskId : tasks.keySet()) {

            if (!visitStates.containsKey(taskId)) {

                detectCycle(taskId, tasks, visitStates);
            }
        }
    }

    private void detectCycle(
            String taskId,
            Map<String, Task> tasks,
            Map<String, VisitState> visitStates
    ) {

        VisitState state = visitStates.get(taskId);

        if (state == VisitState.VISITING) {

            throw new WorkflowValidationException(
                    "Workflow contains a dependency cycle involving task '"
                            + taskId + "'"
            );
        }

        if (state == VisitState.VISITED) {
            return;
        }

        visitStates.put(taskId, VisitState.VISITING);

        Task task = tasks.get(taskId);

        for (String dependencyId : task.getDependencies()) {

            detectCycle(
                    dependencyId,
                    tasks,
                    visitStates
            );
        }

        visitStates.put(taskId, VisitState.VISITED);
    }

    private enum VisitState {
        VISITING,
        VISITED
    }
}