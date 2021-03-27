package science.atlarge.wta.simulator.allocation

import science.atlarge.wta.simulator.model.Task
import science.atlarge.wta.simulator.model.Ticks
import kotlin.math.ceil
import kotlin.math.max

class LookAheadPlacement : TaskPlacementPolicy {

    override fun scheduleTasks(eligibleTasks: Iterator<Task>, callbacks: AllocationCallbacks, currentTime: Ticks) {
        // Compute the total amount of available resources to exit early
        var totalFreeCpu = 0
        callbacks.getMachineStates().forEachRemaining { ms ->
            totalFreeCpu += ms.freeCpus
        }

        // Loop through eligible tasks and try to place them on machines
        while (totalFreeCpu > 0 && eligibleTasks.hasNext()) {
            val task = eligibleTasks.next()

            require(task.earliestStartTime >= 0) {
                "A task had a negative earliestStartTime: ${task.id} had ${task.earliestStartTime}"
            }

            require(currentTime >= task.earliestStartTime) {
                "A task cannot start earlier than its earliest start time. " +
                        "Simulation time was $currentTime and earliest time is ${task.earliestStartTime}} " +
                        "Info: ID: ${task.id} ST: ${task.submissionTime} RT: ${task.runTime}"
            }

            // Update the task slack given some tasks may have been delayed before, eating up slack of this one.
            task.slack = max(0, task.slack - (currentTime - task.earliestStartTime))

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
                            // Two caveats:
                            // 1) runtime of tasks can be 0, so we set these to 1 as it's likely
                            // that given some slack these tasks can then be still delayed significantly
                            // 2) The minimum slowdown of a task is 1.0. We might hit a special case when
                            // A task's runtime = 0 and slack = 0 which would cause a 0 or negative slowdown.
                            max(1.0, ((task.originalRuntime + task.slack - (task.runTime + task.runTime))
                                    / max(1L, task.runTime)).toDouble())
                        )
                    task.runTime = ceil(task.runTime * additionalSlowdown).toLong()
                    task.energyConsumed = task.energyConsumed * (1 - machineState.dvfsOptions[additionalSlowdown]!!)
                }

                callbacks.scheduleTask(task, machineState.machine)
                totalFreeCpu -= task.cpuDemand
                break
            }
        }
    }

}