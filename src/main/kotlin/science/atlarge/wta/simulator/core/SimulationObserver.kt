package science.atlarge.wta.simulator.core

import science.atlarge.wta.simulator.events.Event
import science.atlarge.wta.simulator.events.EventType

abstract class SimulationObserver {

    private val eventHandlers = mutableMapOf<EventType, MutableList<(Event) -> Unit>>()
    val supportedEventTypes: Set<EventType>
        get() = eventHandlers.keys

    @Suppress("UNCHECKED_CAST")
    protected fun <T : Event> registerEventHandler(eventType: EventType, handler: (T) -> Unit) {
        eventHandlers.getOrPut(eventType) { mutableListOf() }.add(handler as (Event) -> Unit)
    }

    fun processEvent(event: Event) {
        val handlers = eventHandlers[event.eventType] ?: throw IllegalArgumentException(
                "Event type ${event.eventType.name} not supported by actor ${idString()}")
        handlers.forEach { it(event) }
    }

    abstract fun idString(): String

}