package science.atlarge.wta.simulator.model

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

    fun getTaskByName(taskName: String): Task {
        return tasksByName[taskName] ?: throw IllegalArgumentException(
                "No task with name \"$taskName\" exists in ${idString()}")
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