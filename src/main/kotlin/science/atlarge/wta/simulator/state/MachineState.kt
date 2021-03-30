package science.atlarge.wta.simulator.state

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import science.atlarge.wta.simulator.model.Machine
import science.atlarge.wta.simulator.model.Task
import java.util.*

class MachineState internal constructor(
    val machine: Machine,
    freeCpus: Int,
    val TDP: Int,
    val dvfsEnabled: Boolean,
    val normalizedSpeed: Double,
    val powerEfficiency: Double
) {

    var freeCpus = freeCpus
        private set

    val dvfsOptions: TreeMap<Double, Double> = TreeMap() // Delay factor to power savings factor
    val taskToNumResources = Int2IntOpenHashMap()

    init {
        // Based on Table 3 in https://www.usenix.org/legacy/events/hotpower08/tech/full_papers/dhiman/dhiman.pdf
        // Average of all workflows and PM-1
        dvfsOptions[1.0] = 0.0
        dvfsOptions[1.2286] = 0.086
        dvfsOptions[1.5344] = 0.126
//        dvfsOptions[2.4728] = 0.124 // We have disabled this one as there is no benefit to it over the previous one.
        require(dvfsOptions.firstKey() >= 1.0) {
            "DVFS slowdown must be >= 1.0 (else it will be a speedup...)"
        }
    }

    constructor(machine: Machine) : this(
        machine,
        machine.numberOfCpus,
        machine.TDP,
        machine.dvfsEnabled,
        machine.normalizedSpeed,
        machine.powerEfficiency
    )

    fun submitTask(task: Task, resources: Int) {
        require(resources <= freeCpus) {
            "Not enough CPUs available on ${machine.idString()} to start ${task.idString()} " +
                    "($freeCpus < ${resources})"
        }
        freeCpus -= resources
        taskToNumResources[task.id] = resources
    }

    fun completeTask(task: Task) {
        require(taskToNumResources.containsKey(task.id)) {
            "${task.idString()} was not running on ${machine.idString()}"
        }
        freeCpus += taskToNumResources[task.id]
        taskToNumResources.remove(task.id)
    }

}