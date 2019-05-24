package science.atlarge.wta.simulator.events

interface WriteOnlyEventQueue {

    fun submit(event: Event)

}