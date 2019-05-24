package science.atlarge.wta.simulator.model

class Environment {

    private val _clusters = mutableListOf<Cluster>()
    private val clustersByName = mutableMapOf<String, Cluster>()
    val clusters: List<Cluster>
        get() = _clusters

    private val _machines = mutableListOf<Machine>()
    val machines: List<Machine>
        get() = _machines

    fun createCluster(name: String): Cluster {
        synchronized(_clusters) {
            require(name !in clustersByName) {
                "Cluster with name $name already exists"
            }
            val newClusterId = _clusters.size
            val newCluster = Cluster(newClusterId, name)
            _clusters.add(newCluster)
            clustersByName[name] = newCluster
            return newCluster
        }
    }

    fun createMachine(name: String, cluster: Cluster, numberOfCpus: Int): Machine {
        require(checkClusterExists(cluster)) {
            "Cannot create machine: ${cluster.idString()} is not part of this environment"
        }
        synchronized(_machines) {
            val newMachineId = _machines.size
            val newMachine = Machine(newMachineId, name, cluster, numberOfCpus)
            _machines.add(newMachine)
            return newMachine
        }
    }

    private fun checkClusterExists(cluster: Cluster): Boolean {
        return cluster.id in _clusters.indices && _clusters[cluster.id] == cluster
    }

}