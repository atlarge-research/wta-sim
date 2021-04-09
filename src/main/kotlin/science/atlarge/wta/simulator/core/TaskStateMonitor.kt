package science.atlarge.wta.simulator.core

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import science.atlarge.wta.simulator.events.*
import science.atlarge.wta.simulator.model.Task
import science.atlarge.wta.simulator.model.Trace
import science.atlarge.wta.simulator.state.SimulationState
import science.atlarge.wta.simulator.state.TaskLifecycle

class TaskStateMonitor(
    private val simulationState: SimulationState,
    private val eventQueue: WriteOnlyEventQueue,
    trace: Trace
) : SimulationObserver() {

    val taskCount = trace.tasks.size
    var pendingSubmissionTaskCount = trace.tasks.size
        private set
    var pendingDependenciesTaskCount = 0
        private set
    var eligibleTaskCount = 0
        private set
    var runningTaskCount = 0
        private set
    var completedTaskCount = 0
        private set
    var energyConsumed: Double = 0.0
        private set

    private val reverseDependencies: Array<Array<Task>>
    private val numberOfMachinesRunningTask = Int2IntOpenHashMap()

    init {
        // Build a reverse dependency map
        val revDeps = Array(trace.tasks.size) { mutableListOf<Task>() }
        for (task in trace.tasks) {
            for (dep in task.dependencies) {
                revDeps[dep.id].add(task)
            }
        }
        reverseDependencies = Array(revDeps.size) { i ->
            revDeps[i].toTypedArray()
        }

        // Register event handlers
        registerEventHandler(EventType.TASK_SUBMITTED, this::taskSubmitted)
        registerEventHandler(EventType.TASK_DEPENDENCIES_MET, this::taskDependenciesMet)
        registerEventHandler(EventType.TASK_STARTED, this::taskStarted)
        registerEventHandler(EventType.TASK_CANCELLED, this::taskCancelled)
        registerEventHandler(EventType.TASK_ATTEMPT_COMPLETED, this::taskAttemptCompleted)
    }

    private fun taskSubmitted(event: TaskSubmittedEvent) {
        val task = event.task
        val taskState = simulationState.of(task)
        // Update task lifecycle
        taskState.taskSubmitted()
        // Check if a dependencies-met event should be emitted
        if (taskState.unmetDepedencies.isEmpty()) {
            eventQueue.submit(TaskDependenciesMetEvent(event.time, task))
        }
        // Update counters
        pendingSubmissionTaskCount--
        pendingDependenciesTaskCount++

    }

    private fun taskDependenciesMet(event: TaskDependenciesMetEvent) {
        // Update counters
        pendingDependenciesTaskCount--
        eligibleTaskCount++
    }

    private fun taskStarted(event: TaskStartedEvent) {
        val task = event.task
        val taskState = simulationState.of(task)
        numberOfMachinesRunningTask.merge(event.task.id, 1, Int::plus)
        if (numberOfMachinesRunningTask[event.task.id] == 1) {
            // Update task lifecycle
            taskState.taskRunning()
            // Update counters
            eligibleTaskCount--
            runningTaskCount++
        }
    }

    private fun taskCancelled(event: TaskCancelledEvent) {
        val task = event.task
        val taskState = simulationState.of(task)
        numberOfMachinesRunningTask[task.id] = numberOfMachinesRunningTask[task.id] - 1
        if (numberOfMachinesRunningTask[task.id] == 0) {
            // Update task lifecycle
            taskState.taskCancelled()
            // Update counters
            runningTaskCount--
            eligibleTaskCount++
            numberOfMachinesRunningTask.remove(event.task.id)
        }
    }

    private fun taskAttemptCompleted(event: TaskAttemptCompletedEvent) {
        val task = event.task
        eventQueue.submit(TaskCompletedEvent(event.time, task, event.machine))
        val taskState = simulationState.of(task)
        // Check if this attempt was cancelled
        if (taskState.taskAttemptNumber != event.attemptNumber) return
        numberOfMachinesRunningTask[task.id] = numberOfMachinesRunningTask[task.id] - 1
        if (numberOfMachinesRunningTask[task.id] == 0) {
            // If not, treat the task as completed
            // Update task lifecycle
            taskState.taskCompleted()
            // Remove task as a dependency of other tasks
            for (dependantTask in reverseDependencies[task.id]) {
                val dependantTaskState = simulationState.of(dependantTask)
                dependantTaskState.dependencyCompleted(task)
                // Check if a dependencies-met event should be emitted
                if (dependantTaskState.unmetDepedencies.isEmpty() && dependantTaskState.lifecycle != TaskLifecycle.TASK_PENDING) {
                    eventQueue.submit(TaskDependenciesMetEvent(event.time, dependantTask))
                }
            }
            // Update counters
            runningTaskCount--
            completedTaskCount++
            energyConsumed += task.energyConsumed
            // Check if an all-tasks-completed event should be emitted
            if (completedTaskCount == taskCount) {
                eventQueue.submit(AllTasksCompletedEvent(event.time))
            }
            numberOfMachinesRunningTask.remove(event.task.id)
        }
    }

    override fun idString(): String {
        return "TaskStateMonitor"
    }

}