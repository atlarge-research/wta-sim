package science.atlarge.wta.simulator.core

import science.atlarge.wta.simulator.events.*
import science.atlarge.wta.simulator.model.Trace
import science.atlarge.wta.simulator.state.SimulationState

class WorkflowStateMonitor(
        private val simulationState: SimulationState,
        private val eventQueue: WriteOnlyEventQueue,
        trace: Trace
) : SimulationObserver() {

    val workflowCount = trace.workflows.size
    var pendingSubmissionWorkflowCount = workflowCount
        private set
    var inProgressWorkflowCount = 0
        private set
    var completedWorkflowCount = 0
        private set
    var energyConsumed = 0
    private set

    init {
        registerEventHandler(EventType.WORKFLOW_SUBMITTED, this::workflowSubmitted)
        registerEventHandler(EventType.TASK_COMPLETED, this::taskCompleted)
        registerEventHandler(EventType.WORKFLOW_COMPLETED, this::workflowCompleted)
    }

    private fun workflowSubmitted(event: WorkflowSubmittedEvent) {
        val workflow = event.workflow
        val workflowState = simulationState.of(workflow)
        // Update workflow lifecycle
        workflowState.workflowSubmitted()
        // Update counters
        pendingSubmissionWorkflowCount--
        inProgressWorkflowCount++
    }

    private fun taskCompleted(event: TaskCompletedEvent) {
        val task = event.task
        val workflow = event.task.workflow ?: return
        val workflowState = simulationState.of(workflow)
        // Update workflow lifecycle
        workflowState.taskCompleted(task)
        // Emit workflow completed event if appropriate
        if (workflowState.remainingTaskCount == 0) {
            eventQueue.submit(WorkflowCompletedEvent(event.time, workflow))
        }
    }

    private fun workflowCompleted(event: WorkflowCompletedEvent) {
        val workflow = event.workflow
        val workflowState = simulationState.of(workflow)
        // Update workflow lifecycle
        workflowState.workflowCompleted()
        // Update counters
        inProgressWorkflowCount--
        completedWorkflowCount++
    }

    override fun idString(): String {
        return "WorkflowStateMonitor"
    }

}