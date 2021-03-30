package science.atlarge.wta.simulator.core

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import science.atlarge.wta.simulator.allocation.QueuedTask
import science.atlarge.wta.simulator.allocation.TaskOrderPolicy
import science.atlarge.wta.simulator.events.*
import science.atlarge.wta.simulator.model.Task
import science.atlarge.wta.simulator.model.Ticks
import science.atlarge.wta.simulator.state.SimulationState
import science.atlarge.wta.simulator.util.AVLTree

class TaskQueue(
        private val simulationState: SimulationState,
        private val eventQueue: WriteOnlyEventQueue,
        taskOrderPolicy: TaskOrderPolicy
) : SimulationObserver() {

    private val tasksEligibleToRun = AVLTree(taskOrderPolicy)
    private val taskToQueueMap = Int2ObjectOpenHashMap<QueuedTask>()
    private var queueExtendedEventEmitted: Boolean = false

    init {
        registerEventHandler(EventType.TASK_DEPENDENCIES_MET, this::addNewTask)
        registerEventHandler(EventType.TASK_CANCELLED, this::addCancelledTask)
        registerEventHandler<Event>(EventType.TASK_QUEUE_EXTENDED) {
            queueExtendedEventEmitted = false
        }
    }

    fun eligibleTaskIterator(): Iterator<Task> = object : Iterator<Task> {
        private val iter = tasksEligibleToRun.iterator()
        override fun hasNext(): Boolean = iter.hasNext()
        override fun next(): Task = iter.next().task
    }

    fun removeScheduledTask(task: Task) {
        val queueEntry = taskToQueueMap.remove(task.id)
        if (queueEntry != null) {  // It might be that a previous machine popped this already
            tasksEligibleToRun.remove(queueEntry)
        }
    }

    private fun addNewTask(event: TaskDependenciesMetEvent) {
        require(event.time >= event.task.submissionTime) {
            "Task should not be added to the queue before its submission time"
        }
        addTask(event.time, event.task)
    }

    private fun addCancelledTask(event: TaskCancelledEvent) {
        addTask(event.time, event.task)
    }

    private fun addTask(time: Ticks, task: Task) {
        // Add a task to the eligible task queue
        val queueEntry = QueuedTask(task, time)
        taskToQueueMap[task.id] = queueEntry
        tasksEligibleToRun.insert(queueEntry)
        // If no task-queue-extended event is currently enqueued, emit one
        if (!queueExtendedEventEmitted) {
            eventQueue.submit(TaskQueueExtendedEvent(simulationState.currentTime))
            queueExtendedEventEmitted = true
        }
    }

    override fun idString(): String {
        return "TaskQueue"
    }

}