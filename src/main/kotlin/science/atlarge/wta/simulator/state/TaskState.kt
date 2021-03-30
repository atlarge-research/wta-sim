package science.atlarge.wta.simulator.state

import science.atlarge.wta.simulator.model.Task

enum class TaskLifecycle {
    TASK_PENDING,
    TASK_SUBMITTED,
    TASK_RUNNING,
    TASK_COMPLETED
}

class TaskState(
        val task: Task
) {

    var lifecycle: TaskLifecycle = TaskLifecycle.TASK_PENDING
        private set

    var taskAttemptNumber: Int = 0
        private set

    private val _unmetDependencies = task.dependencies.toMutableSet()
    val unmetDepedencies: Set<Task>
        get() = _unmetDependencies

    fun taskSubmitted() {
        require(lifecycle == TaskLifecycle.TASK_PENDING) {
            "Task can only start running if it was pending"
        }
        lifecycle = TaskLifecycle.TASK_SUBMITTED
    }

    fun taskRunning() {
        // A task can also be running on another machine that got it earlier.
        // However, all machines should start at the same time, so it cannot be completed.
        require(lifecycle == TaskLifecycle.TASK_SUBMITTED || lifecycle == TaskLifecycle.TASK_RUNNING) {
            "Task can only start running if it was submitted"
        }
        lifecycle = TaskLifecycle.TASK_RUNNING
    }

    fun taskCompleted() {
        // The task could've been completed on another machine that was faster.
        require(lifecycle == TaskLifecycle.TASK_RUNNING || lifecycle == TaskLifecycle.TASK_COMPLETED) {
            "Task can only be completed if it was running"
        }
        lifecycle = TaskLifecycle.TASK_COMPLETED
    }

    fun taskCancelled() {
        // Could've been cancelled on another machine already. TODO: add check - Must happen all at once
        require(lifecycle == TaskLifecycle.TASK_RUNNING || lifecycle == TaskLifecycle.TASK_SUBMITTED) {
            "Task can only be cancelled if it was running"
        }
        lifecycle = TaskLifecycle.TASK_SUBMITTED
    }

    fun dependencyCompleted(dependency: Task) {
        require(_unmetDependencies.remove(dependency)) {
            "${dependency.idString()} is not an unmet dependency of ${task.idString()}"
        }
    }

}