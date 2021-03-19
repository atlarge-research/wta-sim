package science.atlarge.wta.simulator

import science.atlarge.wta.simulator.allocation.*
import science.atlarge.wta.simulator.core.Simulation
import science.atlarge.wta.simulator.input.SamplingTraceReader
import science.atlarge.wta.simulator.input.WTAReader
import science.atlarge.wta.simulator.model.Environment
import science.atlarge.wta.simulator.model.Trace
import science.atlarge.wta.simulator.output.ProgressMonitor
import science.atlarge.wta.simulator.output.SimulationSummary
import science.atlarge.wta.simulator.output.TaskStatsCollector
import science.atlarge.wta.simulator.output.WorkflowStatsCollector
import java.math.BigDecimal
import java.math.BigInteger
import java.math.RoundingMode
import java.util.*
import kotlin.system.measureTimeMillis

object WTASim {

    init {
        TraceReaderRegistry.registerProvider("wta") { WTAReader() }
        TraceReaderRegistry.setDefault("wta")

        TaskPlacementPolicyRegistry.registerProvider("best_fit") { BestFitPlacement() }
        TaskPlacementPolicyRegistry.registerProvider("look_ahead") { LookAheadPlacement() }
        TaskPlacementPolicyRegistry.setDefault("best_fit")

        TaskOrderPolicyRegistry.registerProvider("fcfs") { FirstComeFirstServeOrder() }
        TaskOrderPolicyRegistry.registerProvider("sjf") { ShortestJobFirstOrder() }
        TaskOrderPolicyRegistry.registerProvider("ewf") { EarliestWorkflowFirstOrder() }
        TaskOrderPolicyRegistry.setDefault("fcfs")
    }

    private fun readTrace(cli: CliValues): Trace {
        val traceReader = cli.traceReader
        var hasSampled = false
        if (cli.sampleFraction != null) {
            if (traceReader is SamplingTraceReader) {
                (traceReader as SamplingTraceReader).samplingFraction = cli.sampleFraction
                hasSampled = true
            } else {
                println("WARNING: Parser for specified trace format does not support sampling")
            }
        }
        val trace = traceReader.readTraceFromPaths(cli.tracePath)

        println("--- ${if (hasSampled) "SAMPLED " else ""}TRACE STATS ---")
        println("Number of tasks: ${trace.tasks.size}")
        println("Number of workflows: ${trace.workflows.size}")
        println("Number of dependencies: ${trace.tasks.map { it.dependencies.size }.sum()}")

        return trace
    }

    private fun constructEnvironment(cli: CliValues, trace: Trace): Environment {
        var resourcesPerMachine = cli.cores ?: listOf(1)
        val baseClocks = cli.baseClock ?: List(resourcesPerMachine.size){2.0}
        val dvfsEnabled = cli.dvfsEnabled ?: List(resourcesPerMachine.size){false}
//        val numMachines: Int
        val TDPs = cli.TDPs ?: List(resourcesPerMachine.size){0}
        val machineFractions = cli.machineFractions ?: List(resourcesPerMachine.size){1.0 / resourcesPerMachine.size}

        // Check if the given number of CPUs per machine is sufficient
        val maxResourcesUsed = trace.tasks.map { it.cpuDemand }.max()!!
        // Get the highest clockspeed of all machines
        val highestSpeed = baseClocks.max()!!
        // Compute the normalized speeds of all machines
        val speedFactors = baseClocks.map { s -> s / highestSpeed }

        if (maxResourcesUsed > resourcesPerMachine.max()!!) {
            throw RuntimeException("WARNING: Some tasks in the trace require more than the maximum number of logical cores per machine")
        }

        // Either directly use the specified number of machines, or compute the number of machines needed to achieve
        // the given target utilization
        // TODO Re-enabled the option to run an exact number of machines, per type.
//        if (cli.machines != null) {
//            numMachines = cli.machines
//
//        } else {
            println("--- CONSTRUCTING ENVIRONMENT WITH TARGET UTILIZATION OF ${cli.targetUtilization} ---")

            // Compute the earliest end time of each task to find the "duration" of the trace
        val taskEarliestEndTimes = LongArray(trace.tasks.size) { Long.MIN_VALUE }
        val taskDepCount = IntArray(trace.tasks.size) { i -> trace.tasks[i].dependencies.size }
        val reverseTaskDeps = Array(trace.tasks.size) { mutableSetOf<Int>() }
        trace.tasks.forEach { t -> t.dependencies.forEach { d -> reverseTaskDeps[d.id].add(t.id) } }
        val pendingTasks = Stack<Int>()
        trace.tasks.filter { it.dependencies.isEmpty() }.forEach { pendingTasks.push(it.id) }
        while (pendingTasks.isNotEmpty()) {
            val taskId = pendingTasks.pop()
            val task = trace.getTask(taskId)
            var earliestStartTime = task.submissionTime
            for (dep in task.dependencies) {
                earliestStartTime = maxOf(earliestStartTime, taskEarliestEndTimes[dep.id])
            }
            taskEarliestEndTimes[taskId] = earliestStartTime + task.runTime
            for (rDep in reverseTaskDeps[taskId]) {
                taskDepCount[rDep]--
                if (taskDepCount[rDep] == 0) pendingTasks.push(rDep)
            }
        }

        val traceStartTime = trace.tasks.map { it.submissionTime }.min()!!
        val traceEndTime = taskEarliestEndTimes.max()!!
        val totalResourceUsage = trace.tasks.fold(BigInteger.ZERO) { acc, task ->
            acc.add(BigInteger.valueOf(task.runTime).multiply(BigInteger.valueOf(task.cpuDemand.toLong())))
        }

        // Compute roughly the number of machines to meet the fraction
        val environment = Environment().apply {
            repeat(resourcesPerMachine.size) { i ->
                val cluster = createCluster("Cluster ${i + 1}")
                val numMachines = totalResourceUsage.toBigDecimal().divide(
                        BigDecimal.valueOf(traceEndTime - traceStartTime)
                                .multiply(BigDecimal.valueOf(resourcesPerMachine[i].toLong())) // number of resources
                                .multiply(BigDecimal.valueOf(cli.targetUtilization!!)) // target util
                                .multiply(BigDecimal.valueOf(machineFractions[i].toLong())), // Fraction of this machine
                        32, RoundingMode.CEILING).setScale(0, RoundingMode.CEILING).intValueExact()

                repeat(numMachines) { j ->
                    createMachine("Machine${i + 1}-${j + 1}", cluster, resourcesPerMachine[i], dvfsEnabled[i], speedFactors[i], TDPs[i])
                }
            }
        }

        println("Trace duration: ${traceEndTime - traceStartTime}")
        println("Total CPU usage (cpus * ticks): $totalResourceUsage")
        println("Average CPU usage: ${totalResourceUsage.toBigDecimal().divide(BigDecimal.valueOf(traceEndTime - traceStartTime), 2, RoundingMode.HALF_UP)}")

        println("--- ENVIRONMENT STATS ---")
        println("Number of machines: ${environment.machines.size}")
        println("Number of CPUs per machine: $resourcesPerMachine")
//        println("Number of total CPUs: ${resourcesPerMachine.toLong() * environment.machines.size}")

        return environment
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val cli = parseCliArgs(args)

        val trace = readTrace(cli)
        val environment = constructEnvironment(cli, trace)

        val outputPath = cli.resultPath
        outputPath?.toFile()?.mkdirs()

        println("--- STARTING SIMULATION ---")

        lateinit var taskStats: TaskStatsCollector
        lateinit var workflowStats: WorkflowStatsCollector
        val simTime = measureTimeMillis {
            val simulation = Simulation(environment, trace, cli.taskPlacementPolicy, cli.taskOrderPolicy)
            taskStats = TaskStatsCollector(trace).also { simulation.addSimulationObserver(it) }
            workflowStats = WorkflowStatsCollector(trace, taskStats).also { simulation.addSimulationObserver(it) }
            // TODO: Find better way to inject stats collector
            if (cli.taskOrderPolicy is EarliestWorkflowFirstOrder) {
                cli.taskOrderPolicy.workflowStats = workflowStats
            }
            simulation.addSimulationObserver(ProgressMonitor(simulation.taskStateMonitor))
            simulation.simulate()

            println("--- SIMULATION COMPLETED ---")
        }
        println("Simulation took $simTime milliseconds")

        // Sanity check
        for (task in trace.tasks) {
            val startTime = taskStats.startTimeOf(task)
            val endTime = taskStats.completionTimeOf(task)
            require(startTime >= task.submissionTime)
            if (task.runTime > 0) {
                require(endTime - startTime == task.runTime)
            } else {
                require(endTime == startTime)
            }
            for (dep in task.dependencies) {
                val depEndTime = taskStats.completionTimeOf(dep)
                require(depEndTime <= startTime)
            }
        }
        println("Simulation result passed sanity check")

        if (outputPath != null) {
            println("--- OUTPUTTING SIMULATION RESULTS ---")
            taskStats.writeToFile(outputPath.resolve("tasks.tsv").toFile())
            workflowStats.writeToFile(outputPath.resolve("workflows.tsv").toFile())
            SimulationSummary(trace, taskStats, workflowStats)
                    .writeToFile(outputPath.resolve("summary.tsv").toFile())
        } else {
            println()
            println("Summary of simulation results:")
            println()
            SimulationSummary(trace, taskStats, workflowStats).writeToTerminal()
        }
    }

}

