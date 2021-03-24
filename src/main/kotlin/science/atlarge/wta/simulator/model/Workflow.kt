package science.atlarge.wta.simulator.model

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import java.lang.Long.max
import kotlin.contracts.contract

typealias WorkflowId = Int

class Workflow(
    val id: WorkflowId,
    val name: String
) {

    private val _tasks = mutableListOf<Task>()
    private val tasksByName = mutableMapOf<String, Task>()
    val tasks: List<Task>
        get() = _tasks

    fun addTask(task: Task) {
        require(task.workflow == this) {
            "Can not add task because ${task.idString()} does not belong to ${this.idString()}"
        }
        synchronized(this) {
            if (task.name in tasksByName) {
                require(tasksByName[task.name] == task) {
                    "Task with name \"${task.name}\" already exists in ${idString()}"
                }
            } else {
                _tasks.add(task)
                tasksByName[task.name] = task
            }
        }
    }

    fun computeMinimalStartTimes() {
        // Toposort to compute the optimal start time
        val waves = hashSetOf<HashSet<String>>()

        val depCount = HashMap<String, Int>()
        val children = HashMap<String, HashSet<String>>()
        _tasks.forEach { t ->
            depCount[t.name] = t.dependencies.size
            t.dependencies.forEach {
                children.getOrPut(it.name) { HashSet() }.add(t.name)
            }
        }

        while (true) {
            val wave = HashSet<String>()
            for ((k, v) in depCount) {
                if (v == 0) {
                    wave.add(k)
                }
            }

            if (wave.isEmpty()) {
                break
            }

            for (t in wave) {
                depCount.remove(t)
                children[t]?.forEach { c ->
                    if (depCount.containsKey(c)) {
                        depCount[c] = depCount[c]!! - 1
                    }
                }
            }
            waves.add(wave)
        }

        for (wave in waves) {
            // Update all children
            for (t in wave) {
                val curTask = tasksByName[t]!!
                children[t]?.forEach { c ->
                    val childTask = tasksByName[c]
                    if (childTask != null) {
                        childTask.earliestStartTime = max(childTask.earliestStartTime, curTask.earliestStartTime + curTask.runTime)
                    }
                }
            }
        }

    }

    fun getTaskByName(taskName: String): Task {
        return tasksByName[taskName] ?: throw IllegalArgumentException(
            "No task with name \"$taskName\" exists in ${idString()}"
        )
    }


    override fun equals(other: Any?): Boolean {
        return this === other
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }

    override fun toString(): String {
        return "Workflow(id=$id, name='$name', tasks=[${tasks.joinToString(limit = 3) { it.id.toString() }}])"
    }

    fun idString(): String {
        return "Workflow(id=$id, name='$name')"
    }


}