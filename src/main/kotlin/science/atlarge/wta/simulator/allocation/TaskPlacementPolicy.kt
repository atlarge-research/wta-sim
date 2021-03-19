package science.atlarge.wta.simulator.allocation

import science.atlarge.wta.simulator.model.Machine
import science.atlarge.wta.simulator.model.Task
import science.atlarge.wta.simulator.state.MachineState

interface TaskPlacementPolicy {

    fun scheduleTasks(eligibleTasks: Iterator<Task>, callbacks: AllocationCallbacks)

}

interface AllocationCallbacks {

    fun scheduleTask(task: Task, machine: Machine)

    fun getMachineStates(): Iterator<MachineState>

    fun getMachineStatesByAscendingFreeCpu(): Iterator<MachineState>

    fun getMachineStatesByAscendingFreeCpu(minimumFreeCpu: Int): Iterator<MachineState>

    fun getMachineStatesByAscendingEnergyEfficiency(minimumFreeCpu: Int): Iterator<MachineState>

    fun getMachineStatesByDescendingFreeCpu(): Iterator<MachineState>

}