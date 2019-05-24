package science.atlarge.wta.simulator

import science.atlarge.wta.simulator.allocation.TaskOrderPolicy
import science.atlarge.wta.simulator.allocation.TaskPlacementPolicy
import science.atlarge.wta.simulator.input.TraceReader

abstract class NamedObjectRegistry<T> {

    private var nameOfDefault: String? = null
    private val providersByName = mutableMapOf<String, () -> T>()

    fun getDefault(): T? {
        return when (val s = nameOfDefault) {
            null -> null
            else -> this[s]
        }
    }

    fun setDefault(name: String) {
        nameOfDefault = name
    }

    fun registerProvider(name: String, provider: () -> T) {
        providersByName[name.toLowerCase()] = provider
    }

    operator fun get(name: String): T? {
        return providersByName[name.toLowerCase()]?.invoke()
    }

    operator fun contains(name: String) = name.toLowerCase() in providersByName

    fun listOptions(): List<String> = providersByName.keys.toList()

}

object TaskOrderPolicyRegistry : NamedObjectRegistry<TaskOrderPolicy>()
object TaskPlacementPolicyRegistry : NamedObjectRegistry<TaskPlacementPolicy>()
object TraceReaderRegistry : NamedObjectRegistry<TraceReader>()
