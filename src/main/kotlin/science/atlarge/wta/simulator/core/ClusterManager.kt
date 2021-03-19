package science.atlarge.wta.simulator.core

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
    private var stateChangedEventEmitted: Boolean = false
    private val dummyCluster = Cluster(-1, "X")
    private val dummyMachine = Machine(-1, "X", dummyCluster, Int.MAX_VALUE, false, 1.0, 1)

    init {
        for (machine in environment.machines) {
            val machineState = simulationState.of(machine)
            machinesByFreeCpus.insert(machineState)
        }

        registerEventHandler(EventType.TASK_COMPLETED, this::taskCompleted)
        registerEventHandler(EventType.TASK_CANCELLED, this::taskCancelled)
        registerEventHandler<Event>(EventType.CLUSTER_STATE_CHANGED) {
            stateChangedEventEmitted = false
        }
    }

    fun assignTask(task: Task, machine: Machine) {
        updateMachineState(machine) { it.submitTask(task) }
        broadcastStateChange()
    }

    fun machineStates(): Iterator<MachineState> {
        return environment.machines.asSequence().map(simulationState::of).iterator()
    }

    fun machineStatesByAscendingFreeCpu(): Iterator<MachineState> {
        return machinesByFreeCpus.iterator()
    }

    fun machineStatesByAscendingFreeCpu(minimumFreeCpu: Int): Iterator<MachineState> {
        return machinesByFreeCpus.iteratorFrom(MachineState(dummyMachine, minimumFreeCpu, false, 1.0, 0.0))
    }

    fun machineStatesByAscendingEnergyEfficiency (minimumFreeCpu: Int): Iterator<MachineState> {
        return machinesByFreeCpus.iteratorFrom(MachineState(dummyMachine, minimumFreeCpu, false, 1.0, 0.0))
            .asSequence().sortedBy { it.powerEfficiency }.iterator()
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
        machinesByFreeCpus.remove(machineState)
        fn(machineState)
        machinesByFreeCpus.insert(machineState)
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