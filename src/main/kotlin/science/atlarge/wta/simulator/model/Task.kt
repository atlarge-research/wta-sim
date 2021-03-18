package science.atlarge.wta.simulator.model

import java.util.*

typealias TaskId = Int

class Task(
        val id: TaskId,
        val name: String,
        val workflow: Workflow?,
        val runTime: Ticks,
        val submissionTime: Ticks,
        val slack: Ticks,
        val cpuDemand: Int
) {

    private val _dependencies = mutableSetOf<Task>()
    val dependencies: Set<Task>
        get() = _dependencies
    var energyConsumed: Long

    init {
        workflow?.addTask(this)
        energyConsumed = -1
    }

    fun addDependency(task: Task) {
        require(task.workflow == this.workflow) {
            "Dependencies can only be added between tasks in the same workflow"
        }
        checkForCyclicDependencies(task)?.let { cycle ->
            val cycleString = if (cycle.size <= 5) {
                val builder = StringBuilder()
                for (i in 0 until cycle.size) {
                    if (i != 0) builder.append(" -> ")
                    builder.append(cycle[i].idString())
                }
                builder.toString()
            } else {
                val n = cycle.size - 1
                "${cycle[0].idString()} -> ${cycle[1].idString()} -> ... -> " +
                        "${cycle[n - 1].idString()} -> ${cycle[n].idString()}"
            }
            throw IllegalArgumentException(
                    "Adding a dependency to ${this.idString()} on ${task.idString()} creates a " +
                            "cyclic dependency: $cycleString"
            )
        }
        _dependencies.add(task)
    }

    private fun checkForCyclicDependencies(newDependency: Task): List<Task>? {
        // Special case: self-cycle
        if (newDependency == this) {
            return listOf(this, this)
        }
        // Perform a breadth-first traversal to find the shortest cycle back to this task, if any
        val encounteredTasks = mutableSetOf(newDependency)
        val tasksToCheck = LinkedList<Task>().apply { add(newDependency) }
        val previousTaskInChain = mutableMapOf(newDependency to this)
        while (tasksToCheck.isNotEmpty()) {
            val task = tasksToCheck.removeFirst()
            for (dep in task.dependencies) {
                if (dep == this) {
                    var backtrack = task
                    val cycle = mutableListOf(this, task)
                    while (backtrack != this) {
                        backtrack = previousTaskInChain[backtrack]!!
                        cycle.add(backtrack)
                    }
                    cycle.reverse()
                    return cycle
                } else if (dep !in encounteredTasks) {
                    encounteredTasks.add(dep)
                    tasksToCheck.addLast(dep)
                    previousTaskInChain[dep] = task
                }
            }
        }
        return null
    }

    override fun equals(other: Any?): Boolean {
        return this === other
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }

    override fun toString(): String {
        return "Task(id=$id, name='$name', workflow=${workflow?.id?.toString() ?: "N/A"}, runtime=$runTime, " +
                "submission=$submissionTime, dependencies=[${dependencies
                        .joinToString(limit = 3) { it.id.toString() }}])"
    }

    fun idString(): String {
        return "Task(id=$id, name='$name', workflow=${workflow?.id?.toString() ?: "N/A"})"
    }

}