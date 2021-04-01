package science.atlarge.wta.simulator.allocation

import science.atlarge.wta.simulator.model.Task
import science.atlarge.wta.simulator.model.Ticks
import kotlin.math.ceil
import kotlin.math.max
import kotlin.math.min

class FastestMachinePlacement : TaskPlacementPolicy {

    override fun scheduleTasks(eligibleTasks: Iterator<Task>, callbacks: AllocationCallbacks, currentTime: Ticks) {
        // Compute the total amount of available resources to exit early
        var totalFreeCpu = callbacks.getNumberOfAvailableResources()

        // Loop through eligible tasks and try to place them on machines
        while (totalFreeCpu > 0 && eligibleTasks.hasNext()) {
            val task = eligibleTasks.next()

//            require(task.earliestStartTime >= 0) {
//                "A task had a negative earliestStartTime: ${task.id} had ${task.earliestStartTime}"
//            }
//
//            require(currentTime >= task.earliestStartTime) {
//                "A task cannot start earlier than its earliest start time. " +
//                        "Simulation time was $currentTime and earliest time is ${task.earliestStartTime}} " +
//                        "Info: ID: ${task.id} ST: ${task.submissionTime} RT: ${task.runTime}"
//            }

            // Update the task slack given some tasks may have been delayed before, eating up slack of this one.
            task.slack = max(0, task.slack - (currentTime - task.earliestStartTime))

            var coresLeft = task.cpuDemand

            // Get a list of machines that can fit this task, in ascending order of energy efficiency
            val machineStates = callbacks.getMachineStatesByDescendingMachineSpeed()
            while (coresLeft in 1..totalFreeCpu && machineStates.hasNext()) {
                // Try to place it on the next machine
                val machineState = machineStates.next()

                // Compute the runtime on this machine (in case we do not get assigned the fastest)
                val runTimeOnThisMachine = task.runTime / machineState.normalizedSpeed
                val resourcesToUse = min(machineState.freeCpus, coresLeft)
                val energyConsumptionOnThisMachine = machineState.TDP.toDouble() /
                        machineState.machine.numberOfCpus *
                        resourcesToUse *
                        (runTimeOnThisMachine / 1000 / 3600)  // ms to seconds to hours to get Wh

                // Update task metrics
                task.runTime = max(task.runTime, ceil(runTimeOnThisMachine).toLong())
                task.energyConsumed += energyConsumptionOnThisMachine
                callbacks.scheduleTask(task, machineState.machine, resourcesToUse, resourcesToUse == coresLeft)
                totalFreeCpu -= resourcesToUse
                coresLeft -= resourcesToUse
            }
        }
    }

}