package science.atlarge.wta.simulator.output

import science.atlarge.wta.simulator.core.SimulationObserver
import science.atlarge.wta.simulator.events.*
import science.atlarge.wta.simulator.model.Task
import science.atlarge.wta.simulator.model.Ticks
import science.atlarge.wta.simulator.model.Trace
import java.io.File

// TODO: Instead of collecting all data in memory, spill collected events to disk?
class TaskStatsCollector(
        private val trace: Trace
) : SimulationObserver() {

    private val taskSubmitTimes = LongArray(trace.tasks.size) { Long.MIN_VALUE }
    private val taskReadyTimes = LongArray(trace.tasks.size) { Long.MIN_VALUE }
    private val taskStartedTimes = LongArray(trace.tasks.size) { Long.MIN_VALUE }
    private val taskCompletedTimes = LongArray(trace.tasks.size) { Long.MIN_VALUE }
    private val taskEarliestCompletionTimes = LongArray(trace.tasks.size) { Long.MIN_VALUE }

    init {
        registerEventHandler(EventType.TASK_SUBMITTED, this::taskSubmitted)
        registerEventHandler(EventType.TASK_DEPENDENCIES_MET, this::taskReady)
        registerEventHandler(EventType.TASK_STARTED, this::taskStarted)
        registerEventHandler(EventType.TASK_COMPLETED, this::taskCompleted)
    }

    fun submitTimeOf(task: Task): Ticks {
        val t = taskSubmitTimes[task.id]
        require(t > Long.MIN_VALUE) { "Submit time of task ${task.id} is not known" }
        return t
    }

    fun readyTimeOf(task: Task): Ticks {
        val t = taskReadyTimes[task.id]
        require(t > Long.MIN_VALUE) { "Ready time of task ${task.id} is not known" }
        return t
    }

    fun startTimeOf(task: Task): Ticks {
        val t = taskStartedTimes[task.id]
        require(t > Long.MIN_VALUE) { "Start time of task ${task.id} is not known" }
        return t
    }

    fun completionTimeOf(task: Task): Ticks {
        val t = taskCompletedTimes[task.id]
        require(t > Long.MIN_VALUE) { "Completion time of task ${task.id} is not known" }
        return t
    }

    fun earliestCompletionTimeOf(task: Task): Ticks {
        val t = taskEarliestCompletionTimes[task.id]
        require(t > Long.MIN_VALUE) { "Earliest completion time of task ${task.id} is not known" }
        return t
    }

    private fun taskSubmitted(event: TaskSubmittedEvent) {
        taskSubmitTimes[event.task.id] = event.time
    }

    private fun taskReady(event: TaskDependenciesMetEvent) {
        taskReadyTimes[event.task.id] = event.time
    }

    private fun taskStarted(event: TaskStartedEvent) {
        taskStartedTimes[event.task.id] = event.time
    }

    private fun taskCompleted(event: TaskCompletedEvent) {
        taskCompletedTimes[event.task.id] = event.time
        // Use the task completion order found by the simulator to ensure that we compute the earliest completion
        // of a task *after* all of its dependencies have completed (and their earliest completion time computed)
        var earliestTaskStart = submitTimeOf(event.task)
        for (dep in event.task.dependencies) {
            earliestTaskStart = maxOf(earliestTaskStart, earliestCompletionTimeOf(dep))
        }
        taskEarliestCompletionTimes[event.task.id] = earliestTaskStart + event.task.originalRuntime
    }

    fun writeToFile(outputFile: File) {
        outputFile.bufferedWriter().use { writer ->
            writer.appendln("task.id\tworkflow.id\ttime.submit\ttime.ready\ttime.start\ttime.complete\ttime.runtime.original\ttime.runtime.actual")
            for (task in trace.tasks) {
                writer.append(task.id.toString())
                        .append('\t')
                        .append(task.workflow?.id?.toString() ?: "-1")
                        .append('\t')
                        .append(submitTimeOf(task).toString())
                        .append('\t')
                        .append(readyTimeOf(task).toString())
                        .append('\t')
                        .append(startTimeOf(task).toString())
                        .append('\t')
                        .append(completionTimeOf(task).toString())
                        .append('\t')
                        .append(task.originalRuntime.toString())
                        .append('\t')
                        .append(task.runTime.toString())
                        .appendln()
            }
        }
    }

    override fun idString(): String {
        return "TaskStatsCollector"
    }

}