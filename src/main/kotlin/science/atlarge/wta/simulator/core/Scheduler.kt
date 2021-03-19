package science.atlarge.wta.simulator.core

import science.atlarge.wta.simulator.allocation.AllocationCallbacks
import science.atlarge.wta.simulator.allocation.TaskPlacementPolicy
import science.atlarge.wta.simulator.events.*
import science.atlarge.wta.simulator.model.Machine
import science.atlarge.wta.simulator.model.Task
import science.atlarge.wta.simulator.state.MachineState
import science.atlarge.wta.simulator.state.SimulationState

class Scheduler(
        private val simulationState: SimulationState,
        private val eventQueue: WriteOnlyEventQueue,
        private val taskQueue: TaskQueue,
        private val clusterManager: ClusterManager,
        private val taskPlacementPolicy: TaskPlacementPolicy
) : SimulationObserver() {

    private var rescheduleEventEmitted: Boolean = false
    private val allocationCallbacks = object : AllocationCallbacks {

        override fun scheduleTask(task: Task, machine: Machine) {
            val taskState = simulationState.of(task)
            clusterManager.assignTask(task, machine)
            taskQueue.removeScheduledTask(task)
            eventQueue.submit(TaskStartedEvent(simulationState.currentTime, task, machine))

            // Compute if we can delay the task using slack
            eventQueue.submit(TaskAttemptCompletedEvent(simulationState.currentTime + maxOf(task.runTime, 0L),
                    task, taskState.taskAttemptNumber, machine))
        }

        override fun getMachineStates(): Iterator<MachineState> {
            return clusterManager.machineStates()
        }

        override fun getMachineStatesByAscendingFreeCpu(): Iterator<MachineState> {
            return clusterManager.machineStatesByAscendingFreeCpu()
        }

        override fun getMachineStatesByAscendingFreeCpu(minimumFreeCpu: Int): Iterator<MachineState> {
            return clusterManager.machineStatesByAscendingFreeCpu(minimumFreeCpu)
        }

        override fun getMachineStatesByAscendingEnergyEfficiency(minimumFreeCpu: Int): Iterator<MachineState> {
            return clusterManager.machineStatesByAscendingEnergyEfficiency(minimumFreeCpu)

        }

        override fun getMachineStatesByDescendingFreeCpu(): Iterator<MachineState> {
            return clusterManager.machineStatesByDescendingFreeCpu()
        }

    }

    init {
        registerEventHandler<Event>(EventType.TASK_QUEUE_EXTENDED) { emitRescheduleEvent() }
        registerEventHandler<Event>(EventType.CLUSTER_STATE_CHANGED) { emitRescheduleEvent() }
        registerEventHandler<Event>(EventType.SCHEDULER_RESCHEDULE) { reschedule() }
    }

    private fun reschedule() {
        rescheduleEventEmitted = false
        taskPlacementPolicy.scheduleTasks(taskQueue.eligibleTaskIterator(), allocationCallbacks)
    }

    private fun emitRescheduleEvent() {
        // Emit a reschedule event if one is not already in the event queue
        if (!rescheduleEventEmitted) {
            eventQueue.submit(TriggerRescheduleEvent(simulationState.currentTime))
            rescheduleEventEmitted = true
        }
    }

    override fun idString(): String {
        return "Scheduler"
    }

}