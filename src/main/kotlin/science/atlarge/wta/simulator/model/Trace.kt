package science.atlarge.wta.simulator.model

class Trace {

    private val _workflows = mutableListOf<Workflow>()
    private val workflowsByName = mutableMapOf<String, Workflow>()
    val workflows: List<Workflow>
        get() = _workflows

    private val _tasks = mutableListOf<Task>()
    val tasks: List<Task>
        get() = _tasks

    fun createWorkflow(workflowName: String): Workflow {
        synchronized(_workflows) {
            require(workflowName !in workflowsByName) {
                "Workflow with name $workflowName already exists"
            }
            val newWorkflowId = _workflows.size
            val newWorkflow = Workflow(newWorkflowId, workflowName)
            _workflows.add(newWorkflow)
            workflowsByName[workflowName] = newWorkflow
            return newWorkflow
        }
    }

    fun createTask(
            taskName: String,
            workflow: Workflow?,
            runTime: Ticks,
            submissionTime: Ticks,
            slack: Long,
            cpuDemand: Int
    ): Task {
        require(workflow == null || checkWorkflowExists(workflow)) {
            "Cannot create task: ${workflow!!.idString()} is not part of this trace"
        }
        synchronized(_tasks) {
            val newTaskId = _tasks.size
            val newTask = Task(newTaskId, taskName, workflow, runTime, submissionTime, slack, cpuDemand)
            _tasks.add(newTask)
            return newTask
        }
    }

    fun getWorkflow(id: WorkflowId): Workflow {
        require(id in _workflows.indices) {
            "No workflow found for id $id"
        }
        return _workflows[id]
    }

    fun getWorkflowByName(workflowName: String): Workflow {
        return workflowsByName[workflowName] ?: throw IllegalArgumentException(
                "No workflow with name \"$workflowName\" exists in this trace")
    }

    fun getTask(id: TaskId): Task {
        require(id in _tasks.indices) {
            "No task found for id $id"
        }
        return _tasks[id]
    }

    private fun checkWorkflowExists(workflow: Workflow): Boolean {
        return workflow.id in _workflows.indices && _workflows[workflow.id] == workflow
    }

}