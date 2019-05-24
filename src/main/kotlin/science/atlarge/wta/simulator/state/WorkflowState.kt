package science.atlarge.wta.simulator.state

import science.atlarge.wta.simulator.model.Task
import science.atlarge.wta.simulator.model.Workflow

enum class WorkflowLifecycle {
    WORKFLOW_PENDING,
    WORKFLOW_IN_PROGRESS,
    WORKFLOW_COMPLETED
}

class WorkflowState(
        val workflow: Workflow
) {

    var lifecycle: WorkflowLifecycle = WorkflowLifecycle.WORKFLOW_PENDING
        private set
    var remainingTaskCount: Int = workflow.tasks.size
        private set

    fun workflowSubmitted() {
        require(lifecycle == WorkflowLifecycle.WORKFLOW_PENDING) {
            "Workflow can only be submitted if it was pending"
        }
        lifecycle = WorkflowLifecycle.WORKFLOW_IN_PROGRESS
    }

    fun taskCompleted(task: Task) {
        require(lifecycle == WorkflowLifecycle.WORKFLOW_IN_PROGRESS) {
            "Tasks in workflow can only be completed if the workflow's execution is in progress"
        }
        require(remainingTaskCount > 0) {
            "All tasks in workflow were already completed"
        }
        remainingTaskCount--
    }

    fun workflowCompleted() {
        require(lifecycle == WorkflowLifecycle.WORKFLOW_IN_PROGRESS && remainingTaskCount == 0) {
            "Workflow can only be completed if it was in progress and all of its tasks were completed"
        }
        lifecycle = WorkflowLifecycle.WORKFLOW_COMPLETED
    }

}