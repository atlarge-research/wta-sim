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
        require(lifecycle == TaskLifecycle.TASK_SUBMITTED) {
            "Task can only start running if it was submitted"
        }
        lifecycle = TaskLifecycle.TASK_RUNNING
    }

    fun taskCompleted() {
        require(lifecycle == TaskLifecycle.TASK_RUNNING) {
            "Task can only be completed if it was running"
        }
        lifecycle = TaskLifecycle.TASK_COMPLETED
    }

    fun taskCancelled() {
        require(lifecycle == TaskLifecycle.TASK_RUNNING) {
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