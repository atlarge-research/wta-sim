package science.atlarge.wta.simulator.state

import science.atlarge.wta.simulator.model.Machine
import science.atlarge.wta.simulator.model.Task
import java.util.*

class MachineState internal constructor(
    val machine: Machine,
    freeCpus: Int,
    val TDP: Int,
    val dvfsEnabled: Boolean,
    val speedFactor: Double,
    val powerEfficiency: Double
) {

    var freeCpus = freeCpus
        private set

    val dvfsOptions: TreeMap<Double, Double> = TreeMap() // Delay factor to power savings factor

    init {
        // Based on Table 3 in https://www.usenix.org/legacy/events/hotpower08/tech/full_papers/dhiman/dhiman.pdf
        // Average of all workflows and PM-1
        dvfsOptions[1.0] = 1.0
        dvfsOptions[1.2286] = 1.086
        dvfsOptions[1.5344] = 1.126
//        dvfsOptions[2.4728] = 1.124 // We have disabled this one as there is no benefit to it over the previous one.
    }

    private val _runningTasks = mutableSetOf<Task>()
    val runningTasks: Set<Task>
        get() = _runningTasks

    constructor(machine: Machine) : this(
        machine,
        machine.numberOfCpus,
        machine.TDP,
        machine.dvfsEnabled,
        machine.speedFactor,
        machine.powerEfficiency
    )

    fun submitTask(task: Task) {
        require(task.cpuDemand <= freeCpus) {
            "Not enough CPUs available on ${machine.idString()} to start ${task.idString()} " +
                    "($freeCpus < ${task.cpuDemand})"
        }
        freeCpus -= task.cpuDemand
        _runningTasks.add(task)
    }

    fun completeTask(task: Task) {
        require(_runningTasks.remove(task)) {
            "${task.idString()} was not running on ${machine.idString()}"
        }
        freeCpus += task.cpuDemand
    }

}