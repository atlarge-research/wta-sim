package science.atlarge.wta.simulator.allocation

import science.atlarge.wta.simulator.model.Task
import kotlin.math.ceil

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

            // Get a list of machines that can fit this task, in ascending order of energy efficiency
            val machineStates = callbacks.getMachineStatesByAscendingEnergyEfficiency(task.cpuDemand)
            while (machineStates.hasNext()) {
                // Try to place it on the next machine
                val machineState = machineStates.next()
                // Check if the machine is too slow
                if (task.runTime / machineState.speedFactor > task.runTime + task.slack) {
                    continue
                }

                // At this point we know that we have the most power efficient machine
                // Update the runtime and power consumption
                task.runTime = ceil(task.runTime / machineState.speedFactor).toLong()
                task.energyConsumed = (task.runTime * machineState.TDP).toDouble()

                // Check if DVFS is enabled to see if we can get further gains
                if (machineState.dvfsEnabled) {
                    val additionalSlowdown =
                        machineState.dvfsOptions.floorKey(
                            // Key = leftover slack + runtime / runtime
                            ((task.originalRuntime + task.slack - task.runTime + task.runTime)
                                    / task.runTime).toDouble()
                        )
                    task.runTime = ceil(task.runTime * additionalSlowdown).toLong()
                    task.energyConsumed = task.energyConsumed / machineState.dvfsOptions[additionalSlowdown]!!
                }

                callbacks.scheduleTask(task, machineState.machine)
                totalFreeCpu -= task.cpuDemand
                break
            }
        }
    }

}