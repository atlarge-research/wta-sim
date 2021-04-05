package science.atlarge.wta.simulator.events

import science.atlarge.wta.simulator.model.Machine
import science.atlarge.wta.simulator.model.Task
import science.atlarge.wta.simulator.model.Ticks
import science.atlarge.wta.simulator.model.Workflow

enum class EventType {
    // Note: events in the same tick are processed in the order these constants are defined
    // -- Workflow submission event (handled before any of its tasks are submitted) --
    WORKFLOW_SUBMITTED,
    // -- Task state transition events --
    TASK_SUBMITTED,
    TASK_DEPENDENCIES_MET,
    TASK_STARTED,
    TASK_CANCELLED,
    TASK_ATTEMPT_COMPLETED,
    TASK_COMPLETED,
    // -- Workflow completion event (handled after all of its tasks are completed) --
    WORKFLOW_COMPLETED,
    // -- Task queue events --
    TASK_QUEUE_EXTENDED,
    // -- Environment status events --
    CLUSTER_STATE_CHANGED,
    // -- Scheduler events --
    SCHEDULER_RESCHEDULE,
    // -- State monitoring events --
    // -- Simulation completion events --
    ALL_TASKS_COMPLETED
}

sealed class Event(val time: Ticks) {
    abstract val eventType: EventType
}

class WorkflowSubmittedEvent(time: Ticks, val workflow: Workflow) : Event(time) {
    override val eventType: EventType
        get() = EventType.WORKFLOW_SUBMITTED

    override fun toString(): String {
        return "WorkflowSubmittedEvent(time=$time, workflow=${workflow.idString()})"
    }

}

class WorkflowCompletedEvent(time: Ticks, val workflow: Workflow) : Event(time) {
    override val eventType: EventType
        get() = EventType.WORKFLOW_COMPLETED

    override fun toString(): String {
        return "WorkflowCompletedEvent(time=$time, workflow=${workflow.idString()})"
    }

}

class TaskSubmittedEvent(time: Ticks, val task: Task) : Event(time) {
    override val eventType: EventType
        get() = EventType.TASK_SUBMITTED

    override fun toString(): String {
        return "TaskSubmittedEvent(time=$time, task=${task.idString()})"
    }

}

class TaskDependenciesMetEvent(time: Ticks, val task: Task) : Event(time) {
    override val eventType: EventType
        get() = EventType.TASK_DEPENDENCIES_MET

    override fun toString(): String {
        return "TaskDependenciesMetEvent(time=$time, task=${task.idString()})"
    }

}

class TaskStartedEvent(time: Ticks, val task: Task, val machine: Machine, val resources: Int) : Event(time) {
    override val eventType: EventType
        get() = EventType.TASK_STARTED

    override fun toString(): String {
        return "TaskStartedEvent(time=$time, task=${task.idString()}, machine=${machine.idString()})"
    }

}

class TaskCancelledEvent(time: Ticks, val task: Task, val machine: Machine) : Event(time) {
    override val eventType: EventType
        get() = EventType.TASK_CANCELLED

    override fun toString(): String {
        return "TaskCancelledEvent(time=$time, task=${task.idString()}, machine=${machine.idString()})"
    }

}

class TaskAttemptCompletedEvent(time: Ticks, val task: Task, val attemptNumber: Int, val machine: Machine, val resources: Int) : Event(time) {
    override val eventType: EventType
        get() = EventType.TASK_ATTEMPT_COMPLETED

    override fun toString(): String {
        return "TaskAttemptCompletedEvent(time=$time, task=${task.idString()}, attempt=${attemptNumber}, machine=${machine.idString()})"
    }

}

class TaskCompletedEvent(time: Ticks, val task: Task, val machine: Machine) : Event(time) {
    override val eventType: EventType
        get() = EventType.TASK_COMPLETED

    override fun toString(): String {
        return "TaskCompletedEvent(time=$time, task=${task.idString()}, machine=${machine.idString()})"
    }

}

class TaskQueueExtendedEvent(time: Ticks) : Event(time) {
    override val eventType: EventType
        get() = EventType.TASK_QUEUE_EXTENDED

    override fun toString(): String {
        return "TaskQueueExtendedEvent(time=$time)"
    }

}

class ClusterStateChangedEvent(time: Ticks) : Event(time) {
    override val eventType: EventType
        get() = EventType.CLUSTER_STATE_CHANGED

    override fun toString(): String {
        return "ClusterStateChangedEvent(time=$time)"
    }

}

class TriggerRescheduleEvent(time: Ticks) : Event(time) {
    override val eventType: EventType
        get() = EventType.SCHEDULER_RESCHEDULE

    override fun toString(): String {
        return "TriggerRescheduleEvent(time=$time)"
    }

}

class AllTasksCompletedEvent(time: Ticks) : Event(time) {
    override val eventType: EventType
        get() = EventType.ALL_TASKS_COMPLETED

    override fun toString(): String {
        return "AllTasksCompletedEvent(time=$time)"
    }

}
