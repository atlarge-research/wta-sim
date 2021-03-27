package science.atlarge.wta.simulator.core

import science.atlarge.wta.simulator.allocation.TaskOrderPolicy
import science.atlarge.wta.simulator.allocation.TaskPlacementPolicy
import science.atlarge.wta.simulator.events.*
import science.atlarge.wta.simulator.model.Environment
import science.atlarge.wta.simulator.model.Ticks
import science.atlarge.wta.simulator.model.Trace
import science.atlarge.wta.simulator.state.SimulationState

class Simulation(
    val environment: Environment,
    val trace: Trace,
    taskPlacementPolicy: TaskPlacementPolicy,
    taskOrderPolicy: TaskOrderPolicy
) {

    private val simulationState = object : SimulationState(trace, environment, Long.MIN_VALUE) {
        fun setTime(time: Ticks) {
            this.currentTime = time
        }
    }

    private val eventQueue = EventQueue()
    val taskStateMonitor = TaskStateMonitor(simulationState, eventQueue, trace)
    val workflowStateMonitor = WorkflowStateMonitor(simulationState, eventQueue, trace)
    private val clusterManager = ClusterManager(simulationState, eventQueue, environment)
    private val taskQueue = TaskQueue(simulationState, eventQueue, taskOrderPolicy)
    private val scheduler = Scheduler(simulationState, eventQueue, taskQueue, clusterManager, taskPlacementPolicy)

    private var isSimulationDone = false

    private val actor = object : SimulationObserver() {

        init {
            registerEventHandler<Event>(EventType.ALL_TASKS_COMPLETED) { isSimulationDone = true }
        }

        override fun idString(): String {
            return "Simulation"
        }

    }

    private val eventHandlers = Array(EventType.values().size) { mutableListOf<SimulationObserver>() }

    init {
        listOf(workflowStateMonitor, taskStateMonitor, clusterManager, taskQueue, scheduler, actor)
            .forEach(this::addSimulationObserver)
    }

    fun addSimulationObserver(observer: SimulationObserver) {
        for (et in observer.supportedEventTypes) {
            eventHandlers[et.ordinal].add(observer)
        }
    }

    fun simulate() {
        // Create a workflow submission event for every workflow
        for (workflow in trace.workflows) {
            // The submission time of a workflow is defined as the submission time of the
            // first entry task submitted to the system

            // Some workflows were filtered out by the slack computation, workflows with just 1 task are
            // considered a Bag of tasks. Skip those.
            if (workflow.tasks.isEmpty()) {
                continue
            }

            val submitTime = workflow.tasks
                .filter { it.dependencies.isEmpty() }
                .map { it.submissionTime }.min()
                ?: throw IllegalArgumentException("A workflow must consist of at least one task")
            eventQueue.submit(WorkflowSubmittedEvent(submitTime, workflow))
        }
        // Create a task submission event for every task
        trace.tasks.forEach { t -> eventQueue.submit(TaskSubmittedEvent(t.submissionTime, t)) }
        // Process events until the simulation is done
        while (!isSimulationDone && eventQueue.isNotEmpty) {
            val event = eventQueue.pop()!!
            require(event.time >= simulationState.currentTime) {
                "Current time is ${simulationState.currentTime} but received an event at time ${event.time}"
            }
            simulationState.setTime(event.time)
            for (eh in eventHandlers[event.eventType.ordinal]) {
                eh.processEvent(event)
            }
        }
        // Check that all tasks have completed
        require(taskStateMonitor.completedTaskCount == taskStateMonitor.taskCount) {
            "Not all tasks were completed"
        }
    }

}