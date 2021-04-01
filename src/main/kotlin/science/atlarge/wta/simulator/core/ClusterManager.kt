package science.atlarge.wta.simulator.core

import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap
import science.atlarge.wta.simulator.events.*
import science.atlarge.wta.simulator.model.Cluster
import science.atlarge.wta.simulator.model.Environment
import science.atlarge.wta.simulator.model.Machine
import science.atlarge.wta.simulator.model.Task
import science.atlarge.wta.simulator.state.MachineState
import science.atlarge.wta.simulator.state.SimulationState
import science.atlarge.wta.simulator.util.AVLTree

class ClusterManager(
        private val simulationState: SimulationState,
        private val eventQueue: WriteOnlyEventQueue,
        private val environment: Environment
) : SimulationObserver() {

    private val machinesByFreeCpus = AVLTree(compareBy(MachineState::freeCpus, { it.machine.id }))
    private val freeMachinesByPowerEfficiency = AVLTree(compareBy(MachineState::powerEfficiency, { it.machine.id }))
    private val freeMachinesBySpeed = AVLTree(compareBy<MachineState>({ -it.normalizedSpeed }, { it.machine.id }))
    private var stateChangedEventEmitted: Boolean = false
    private val dummyCluster = Cluster(-1, "X")
    private val dummyMachine = Machine(-1, "X", dummyCluster, Int.MAX_VALUE, false, 1.0, 1)
    public var numberOfFreeResources = 0
    public var resourcesAvailablePerMachineSpeed = Double2IntOpenHashMap()

    init {
        for (machine in environment.machines) {
            numberOfFreeResources += machine.numberOfCpus
            resourcesAvailablePerMachineSpeed.merge(machine.normalizedSpeed, machine.numberOfCpus, Int::plus)
            val machineState = simulationState.of(machine)
            machinesByFreeCpus.insert(machineState)
            freeMachinesByPowerEfficiency.insert(machineState)
            freeMachinesBySpeed .insert(machineState)
        }

        registerEventHandler(EventType.TASK_COMPLETED, this::taskCompleted)
        registerEventHandler(EventType.TASK_CANCELLED, this::taskCancelled)
        registerEventHandler<Event>(EventType.CLUSTER_STATE_CHANGED) {
            stateChangedEventEmitted = false
        }
    }

    fun assignTask(task: Task, machine: Machine, resources: Int) {
        updateMachineState(machine) { it.submitTask(task, resources) }
        broadcastStateChange()
    }

    fun machineStates(): Iterator<MachineState> {
        return environment.machines.asSequence().map(simulationState::of).iterator()
    }

    fun machineStatesByAscendingFreeCpu(): Iterator<MachineState> {
        return machinesByFreeCpus.iterator()
    }

    fun machineStatesByAscendingFreeCpu(minimumFreeCpu: Int): Iterator<MachineState> {
        return machinesByFreeCpus.iteratorFrom(MachineState(dummyMachine, minimumFreeCpu, 1, false, 1.0, 0.0))
    }

    fun machineStatesByAscendingEnergyEfficiency (): Iterator<MachineState> {
        return freeMachinesByPowerEfficiency.iterator()
    }

    fun machineStatesByDescendingSpeed (): Iterator<MachineState> {
        return freeMachinesBySpeed.iterator()
    }

    fun machineStatesByDescendingFreeCpu(): Iterator<MachineState> {
        return machinesByFreeCpus.reverseIterator()
    }

    private fun taskCompleted(event: TaskCompletedEvent) {
        updateMachineState(event.machine) { it.completeTask(event.task) }
        broadcastStateChange()
    }

    private fun taskCancelled(event: TaskCompletedEvent) {
        updateMachineState(event.machine) { it.completeTask(event.task) }
        broadcastStateChange()
    }

    private fun updateMachineState(machine: Machine, fn: (MachineState) -> Unit) {
        val machineState = simulationState.of(machine)
        val wasMachineFree = machineState.freeCpus > 0
        numberOfFreeResources -= machineState.freeCpus
        resourcesAvailablePerMachineSpeed.merge(machineState.normalizedSpeed, machineState.freeCpus, Int::minus)
        machinesByFreeCpus.remove(machineState)
        fn(machineState)
        numberOfFreeResources += machineState.freeCpus
        resourcesAvailablePerMachineSpeed.merge(machineState.normalizedSpeed, machineState.freeCpus, Int::plus)
        machinesByFreeCpus.insert(machineState)
        // Remove if machine is now no longer free, add if machine has become free
        if (wasMachineFree && machineState.freeCpus == 0) {
            freeMachinesByPowerEfficiency.remove(machineState)
            freeMachinesBySpeed.remove(machineState)
        } else if (!wasMachineFree && machineState.freeCpus > 0) {
            freeMachinesByPowerEfficiency.insert(machineState)
            freeMachinesBySpeed.insert(machineState)
        }
    }

    private fun broadcastStateChange() {
        // If no cluster-state-changed event is currently enqueued, emit one
        if (!stateChangedEventEmitted) {
            eventQueue.submit(ClusterStateChangedEvent(simulationState.currentTime))
            stateChangedEventEmitted = true
        }
    }

    override fun idString(): String {
        return "ClusterManager"
    }

}