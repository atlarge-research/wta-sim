package science.atlarge.wta.simulator.events

import java.util.*

class EventQueue : WriteOnlyEventQueue {

    private val events = PriorityQueue<Event>(compareBy({ it.time }, { it.eventType.ordinal }))

    val size: Int
        get() = events.size
    val isEmpty: Boolean
        get() = size == 0
    val isNotEmpty: Boolean
        get() = !isEmpty

    override fun submit(event: Event) {
        events.add(event)
    }

    fun peek(): Event? {
        return events.peek()
    }

    fun pop(): Event? {
        return events.poll()
    }

}