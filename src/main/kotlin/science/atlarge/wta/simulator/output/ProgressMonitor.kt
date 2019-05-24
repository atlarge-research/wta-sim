package science.atlarge.wta.simulator.output

import science.atlarge.wta.simulator.core.SimulationObserver
import science.atlarge.wta.simulator.core.TaskStateMonitor
import science.atlarge.wta.simulator.events.Event
import science.atlarge.wta.simulator.events.EventType

class ProgressMonitor(
        private val taskStateMonitor: TaskStateMonitor
) : SimulationObserver() {

    private var eventCount = 0L
    private var eventsUntilTimeCheck = EVENTS_BETWEEN_TIME_CHECK
    private var lastPrintTimestamp = System.currentTimeMillis()
    private var lastPrintedEventCount = 0L

    init {
        for (v in EventType.values()) {
            registerEventHandler(v, this::countEvent)
        }
    }

    private fun countEvent(event: Event) {
        eventCount++
        eventsUntilTimeCheck--
        if (eventsUntilTimeCheck == 0 || event.eventType == EventType.ALL_TASKS_COMPLETED) {
            val currentTime = System.currentTimeMillis()
            if (currentTime >= lastPrintTimestamp + MILLIS_BETWEEN_PRINTING ||
                    event.eventType == EventType.ALL_TASKS_COMPLETED) {
                println("Processed ${eventCount - lastPrintedEventCount} events in ${currentTime - lastPrintTimestamp} ms")
                println("  Tasks submitted/eligible/running/completed: ${taskStateMonitor.pendingDependenciesTaskCount}/" +
                        "${taskStateMonitor.eligibleTaskCount}/${taskStateMonitor.runningTaskCount}/${taskStateMonitor.completedTaskCount}")
                lastPrintTimestamp = currentTime
                lastPrintedEventCount = eventCount
            }
            eventsUntilTimeCheck = EVENTS_BETWEEN_TIME_CHECK
        }
    }

    override fun idString(): String {
        return "ProgressMonitor"
    }

    companion object {
        private const val EVENTS_BETWEEN_TIME_CHECK = 100000
        private const val MILLIS_BETWEEN_PRINTING = 10000
    }

}