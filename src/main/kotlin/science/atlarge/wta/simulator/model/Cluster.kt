package science.atlarge.wta.simulator.model

typealias ClusterId = Int

class Cluster(
        val id: ClusterId,
        val name: String
) {

    private val _machines = mutableListOf<Machine>()
    private val machinesByName = mutableMapOf<String, Machine>()
    val machines: List<Machine>
        get() = _machines

    fun addMachine(machine: Machine) {
        require(machine.cluster == this) {
            "Can not add ${machine.idString()} because it does not belong to ${this.idString()}"
        }
        synchronized(this) {
            if (machine.name in machinesByName) {
                require(machinesByName[machine.name] == machine) {
                    "Machine with name \"${machine.name}\" already exists in ${idString()}"
                }
            } else {
                _machines.add(machine)
                machinesByName[machine.name] = machine
            }
        }
    }

    fun getMachineByName(machineName: String): Machine {
        return machinesByName[machineName] ?: throw IllegalArgumentException(
                "No machine with name \"$machineName\" exists in ${idString()}")
    }

    override fun equals(other: Any?): Boolean {
        return this === other
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }

    override fun toString(): String {
        return "Cluster(id=$id, name='$name', machines=[${machines.joinToString(limit = 3) { it.id.toString() }}])"
    }

    fun idString(): String {
        return "Cluster(id=$id, name='$name')"
    }

}