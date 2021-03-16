package science.atlarge.wta.simulator.allocation

import science.atlarge.wta.simulator.model.Task

class LookAheadPlacement : TaskPlacementPolicy {

    override fun scheduleTasks(eligibleTasks: Iterator<Task>, callbacks: AllocationCallbacks) {
        // Compute the total amount of available resources to exit early
        var totalFreeCpu = 0
        callbacks.getMachineStates().forEachRemaining { ms ->
            totalFreeCpu += ms.freeCpus
        }

        // Loop through eligible tasks and try to place them on machines
        while (totalFreeCpu > 0 && eligibleTasks.hasNext()) {
            val task = eligibleTasks.next()

            // Get a list of machines that can fit this task, in ascending order of available resources
            val machineStates = callbacks.getMachineStatesByAscendingFreeCpu(task.cpuDemand)
            while (machineStates.hasNext()) {
                // Try to place it on the next machine
                // NOTE: Currently always works, because there are no other requirements than number of free resources
                val machineState = machineStates.next()
                callbacks.scheduleTask(task, machineState.machine)
                totalFreeCpu -= task.cpuDemand
                break
            }
        }
    }

}