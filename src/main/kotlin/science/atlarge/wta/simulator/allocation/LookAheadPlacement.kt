package science.atlarge.wta.simulator.allocation

import science.atlarge.wta.simulator.model.Task
import science.atlarge.wta.simulator.model.Ticks
import kotlin.math.ceil
import kotlin.math.max
import kotlin.math.min

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

            var coresLeft = task.cpuDemand

            // Get a list of machines that can fit this task, in ascending order of energy efficiency
            val machineStates = callbacks.getMachineStatesByAscendingEnergyEfficiency()
            while (coresLeft in 1..totalFreeCpu && machineStates.hasNext()) {
                // Try to place it on the next machine
                val machineState = machineStates.next()
                // Check if the machine is too slow
                if (task.runTime / machineState.normalizedSpeed > task.runTime + task.slack) {
                    continue
                }

                // Set the runtime to the slowest machine
                var runTimeOnThisMachine = task.runTime / machineState.normalizedSpeed
                val resourcesToUse = min(machineState.freeCpus, coresLeft)
                var energyConsumptionOnThisMachine = runTimeOnThisMachine *
                        (machineState.TDP.toDouble() / machineState.machine.numberOfCpus) *
                        resourcesToUse

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
                            max(1.0, ((task.originalRuntime + task.slack - (2*runTimeOnThisMachine))
                                    / max(1.0, runTimeOnThisMachine)).toDouble())
                        )
                    runTimeOnThisMachine *= additionalSlowdown
                    energyConsumptionOnThisMachine *= (1 - machineState.dvfsOptions[additionalSlowdown]!!)
                }

                // Update task metrics
                task.runTime = max(task.runTime, ceil(runTimeOnThisMachine).toLong())
                task.energyConsumed += energyConsumptionOnThisMachine
                callbacks.scheduleTask(task, machineState.machine, resourcesToUse)
                totalFreeCpu -= resourcesToUse
                coresLeft -= resourcesToUse
            }
        }
    }

}