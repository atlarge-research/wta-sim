package science.atlarge.wta.simulator.state

import science.atlarge.wta.simulator.model.*

abstract class SimulationState(
        private val trace: Trace,
        private val environment: Environment,
        initialTime: Ticks
) {

    var currentTime: Ticks = initialTime
        protected set

    private val workflowStates = Array(trace.workflows.size) { i -> WorkflowState(trace.workflows[i]) }
    private val taskStates = Array(trace.tasks.size) { i -> TaskState(trace.tasks[i]) }
    private val machineStates = Array(environment.machines.size) { i -> MachineState(environment.machines[i]) }

    fun ofWorkflow(workflowId: WorkflowId): WorkflowState {
        return workflowStates[workflowId]
    }

    fun of(workflow: Workflow): WorkflowState {
        return ofWorkflow(workflow.id)
    }

    fun ofTask(taskId: TaskId): TaskState {
        return taskStates[taskId]
    }

    fun of(task: Task): TaskState {
        return ofTask(task.id)
    }

    fun ofMachine(machineId: MachineId): MachineState {
        return machineStates[machineId]
    }

    fun of(machine: Machine): MachineState {
        return ofMachine(machine.id)
    }

}