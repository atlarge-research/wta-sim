package science.atlarge.wta.simulator.allocation

import science.atlarge.wta.simulator.model.Task
import science.atlarge.wta.simulator.model.Ticks

class BestFitPlacement : TaskPlacementPolicy {

    override fun scheduleTasks(eligibleTasks: Iterator<Task>, callbacks: AllocationCallbacks, currentTime: Ticks) {
        // Compute the total amount of available resources to exit early
        var totalFreeCpu =  callbacks.getNumberOfAvailableResources()

        // Loop through eligible tasks and try to place them on machines
        while (totalFreeCpu > 0 && eligibleTasks.hasNext()) {
            val task = eligibleTasks.next()

            // Get a list of machines that can fit this task, in ascending order of available resources
            val machineStates = callbacks.getMachineStatesByAscendingFreeCpu(task.cpuDemand)
            while (machineStates.hasNext()) {
                // Try to place it on the next machine
                // NOTE: Currently always works, because there are no other requirements than number of free resources
                val machineState = machineStates.next()
                // TODO change task.cpuDemand such that we take the minimum of the machine's resources and that
                // task requirements!
                // TODO change true to check if the taks is completely scheduled if spread across multiple machines
                callbacks.scheduleTaskOnMachine(task, machineState.machine, task.cpuDemand, true)
                totalFreeCpu -= task.cpuDemand
                break
            }
        }
    }

}