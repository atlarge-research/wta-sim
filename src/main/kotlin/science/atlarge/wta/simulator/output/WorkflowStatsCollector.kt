package science.atlarge.wta.simulator.output

import science.atlarge.wta.simulator.core.SimulationObserver
import science.atlarge.wta.simulator.events.EventType
import science.atlarge.wta.simulator.events.WorkflowCompletedEvent
import science.atlarge.wta.simulator.events.WorkflowSubmittedEvent
import science.atlarge.wta.simulator.model.Ticks
import science.atlarge.wta.simulator.model.Trace
import science.atlarge.wta.simulator.model.Workflow
import java.io.File

class WorkflowStatsCollector(
        private val trace: Trace,
        private val taskStatsCollector: TaskStatsCollector
) : SimulationObserver() {

    private val workflowSubmitTimes = LongArray(trace.workflows.size) { Long.MIN_VALUE }
    private val workflowCompletedTimes = LongArray(trace.workflows.size) { Long.MIN_VALUE }
    private val workflowEarliestCompletionTimes = LongArray(trace.workflows.size) { Long.MIN_VALUE }

    init {
        registerEventHandler(EventType.WORKFLOW_SUBMITTED, this::workflowSubmitted)
        registerEventHandler(EventType.WORKFLOW_COMPLETED, this::workflowCompleted)
    }

    fun submitTimeOf(workflow: Workflow): Ticks {
        val t = workflowSubmitTimes[workflow.id]
        require(t >= 0) { "Submit time of workflow ${workflow.id} is not known" }
        return t
    }

    fun completionTimeOf(workflow: Workflow): Ticks {
        val t = workflowCompletedTimes[workflow.id]
        require(t > Long.MIN_VALUE) { "Completion time of workflow ${workflow.id} is not known" }
        return t
    }

    fun earliestCompletionTimeOf(workflow: Workflow): Ticks {
        val t = workflowEarliestCompletionTimes[workflow.id]
        require(t > Long.MIN_VALUE) { "Earliest completion time of workflow ${workflow.id} is not known" }
        return t
    }

    private fun workflowSubmitted(event: WorkflowSubmittedEvent) {
        workflowSubmitTimes[event.workflow.id] = event.time
    }

    private fun workflowCompleted(event: WorkflowCompletedEvent) {
        workflowCompletedTimes[event.workflow.id] = event.time
        // Compute the earliest possible completion time of this workflow
        workflowEarliestCompletionTimes[event.workflow.id] = event.workflow.tasks
                .map { taskStatsCollector.earliestCompletionTimeOf(it) }
                .max()!!
    }

    fun writeToFile(outputFile: File) {
        outputFile.bufferedWriter().use { writer ->
            writer.appendln("workflow.id\ttime.submit\ttime.complete\ttime.earliest.complete")
            for (workflow in trace.workflows) {
                writer.append(workflow.id.toString())
                        .append('\t')
                        .append(submitTimeOf(workflow).toString())
                        .append('\t')
                        .append(completionTimeOf(workflow).toString())
                        .append('\t')
                        .append(earliestCompletionTimeOf(workflow).toString())
                        .appendln()
            }
        }
    }

    override fun idString(): String {
        return "WorkflowStatsCollector"
    }

}