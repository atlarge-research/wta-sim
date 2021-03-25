package science.atlarge.wta.simulator

import org.apache.commons.cli.*
import science.atlarge.wta.simulator.allocation.TaskOrderPolicy
import science.atlarge.wta.simulator.allocation.TaskPlacementPolicy
import science.atlarge.wta.simulator.input.TraceReader
import java.nio.file.InvalidPathException
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.system.exitProcess

data class CliValues(
        val tracePath: Path,
        val slackFolder: Path,
        val resultPath: Path?,
        val machines: Int?,
        val cores: List<Int>?,
        val dvfsEnabled: List<Boolean>?,
        val TDPs: List<Int>?,
        val baseClock: List<Double>?,
        val targetUtilization: Double?,
        val machineFractions: List<Double>?,
        val sampleFraction: Double?,
        val traceReader: TraceReader,
        val taskPlacementPolicy: TaskPlacementPolicy,
        val taskOrderPolicy: TaskOrderPolicy
)

fun parseCliArgs(args: Array<String>): CliValues {
    val cliParser = DefaultParser()
    val helpFormatter = HelpFormatter()
    try {
        val cmd = cliParser.parse(CliOptions.allOptions, args)

        // Extract option values from the command line
        val tracePathStr = cmd.getOptionString(CliOptions.tracePath)
        val slackPathStr = cmd.getOptionString(CliOptions.slackPath)
        val traceFormatStr = cmd.getOptionStringOrNull(CliOptions.traceFormat)
        val sampleFraction = cmd.getOptionDoubleOrNull(CliOptions.sampleFraction)
        val resultPathStr = cmd.getOptionStringOrNull(CliOptions.resultDir)
        val machines = cmd.getOptionIntOrNull(CliOptions.machines)
        val cores = cmd.getIntArrayOrNull(CliOptions.cores)
        val baseClocks = cmd.getDoubleArrayOrNull(CliOptions.baseClocks)
        val dvfsEnabled = cmd.getBooleanArrayOrNull(CliOptions.dvfsEnabled)
        val TDPs = cmd.getIntArrayOrNull(CliOptions.TDPs)
        val targetUtilization = cmd.getOptionDoubleOrNull(CliOptions.targetUtilization)
        val machineFractions = cmd.getDoubleArrayOrNull(CliOptions.machineFractions)
        val taskPlacementPolicyStr = cmd.getOptionStringOrNull(CliOptions.taskPlacementPolicy)
        val taskOrderPolicyStr = cmd.getOptionStringOrNull(CliOptions.taskOrderPolicy)

        // Parse and sanity check options
        // - Input description
        val tracePath = parseTracePath(tracePathStr)
        val slackPath = parseTracePath(slackPathStr)
        val traceReader = parseTraceFormat(traceFormatStr)
        if (sampleFraction != null && (sampleFraction <= 0 || sampleFraction > 1))
            throw ParseException("Sample fraction must be in the range (0, 1], was: $sampleFraction")
        // - Output description
        val resultPath = parseResultPath(resultPathStr)
        // - Simulated environment
        if ((machines == null && targetUtilization == null) || (machines != null && targetUtilization != null))
            throw ParseException("Exactly one of the machines and target-utilization options must be specified")
        if (machines != null && machines <= 0)
            throw ParseException("A non-negative number of machines is required (was: $machines)")
        if (targetUtilization != null && (targetUtilization <= 0 || targetUtilization > 1))
            throw ParseException("Target utilization must be in the range (0, 1], was: $targetUtilization")
        if(cores == null)
            throw ParseException("Need to supply core sizes")
        if (cores.isEmpty())
            throw ParseException("Need to supply core sizes")
        if (cores.min()!! <= 0)
            throw ParseException("A non-negative number of cores is required (was: ${cores.min()})")
        if(baseClocks == null)
            throw ParseException("Need to supply base clocks")
        if (baseClocks.isEmpty())
            throw ParseException("Need to supply base clock speeds")
        if (baseClocks.min()!! <= 0)
            throw ParseException("A non-negative number of base clock is required (was: ${cores.min()})")
        if(baseClocks.size != cores.size)
            throw ParseException("Cores and base clock list sizes mismatch: ${cores.size} vs ${baseClocks.size}")
        if(TDPs != null && TDPs.size != cores.size)
            throw ParseException("Cores and TDPs list sizes mismatch: ${cores.size} vs ${TDPs.size}")
        if(TDPs != null && TDPs.min()!! <= 0)
            throw ParseException("Must have positive TDP values")
        // - Scheduler configuration
        val taskPlacementPolicy = parseTaskPlacementPolicy(taskPlacementPolicyStr)
        val taskOrderPolicy = parseTaskOrderPolicy(taskOrderPolicyStr)

        return CliValues(tracePath, slackPath, resultPath, machines, cores, dvfsEnabled, TDPs, baseClocks, targetUtilization, machineFractions, sampleFraction, traceReader,
                taskPlacementPolicy, taskOrderPolicy)
    } catch (e: ParseException) {
        println(e.message)
        helpFormatter.printHelp("WTASim", CliOptions.allOptions)
        exitProcess(1)
    }
}

private fun parseTracePath(tracePathStr: String): Path {
    val tracePath = try {
        Paths.get(tracePathStr)
    } catch (e: InvalidPathException) {
        throw ParseException("Failed to parse specified input path: $e")
    }
    if (!tracePath.toFile().exists()) throw ParseException("Specified input path does not exist: $tracePathStr")
    return tracePath
}

private fun parseTraceFormat(traceFormatStr: String?): TraceReader {
    if (traceFormatStr == null)
        return TraceReaderRegistry.getDefault()
                ?: throw IllegalStateException("Missing default TraceReader")
    return TraceReaderRegistry[traceFormatStr]
            ?: throw ParseException("Option trace-format must be one of [" +
                    "${TraceReaderRegistry.listOptions().joinToString()}] (was: $traceFormatStr)")
}

private fun parseResultPath(resultPathStr: String?): Path? {
    if (resultPathStr == null) return null
    try {
        return Paths.get(resultPathStr)
    } catch (e: InvalidPathException) {
        throw ParseException("Failed to parse specified output path: $e")
    }
}

private fun parseTaskPlacementPolicy(taskPlacementPolicyStr: String?): TaskPlacementPolicy {
    if (taskPlacementPolicyStr == null)
        return TaskPlacementPolicyRegistry.getDefault()
                ?: throw IllegalStateException("Missing default TaskPlacementPolicy")
    return TaskPlacementPolicyRegistry[taskPlacementPolicyStr]
            ?: throw ParseException("Option task-placement-policy must be one of [" +
                    "${TaskPlacementPolicyRegistry.listOptions().joinToString()}] (was: $taskPlacementPolicyStr)")
}

private fun parseTaskOrderPolicy(taskOrderPolicyStr: String?): TaskOrderPolicy {
    if (taskOrderPolicyStr == null)
        return TaskOrderPolicyRegistry.getDefault()
                ?: throw IllegalStateException("Missing default TaskOrderPolicy")
    return TaskOrderPolicyRegistry[taskOrderPolicyStr]
            ?: throw ParseException("Option task-order-policy must be one of [" +
                    "${TaskOrderPolicyRegistry.listOptions().joinToString()}] (was: $taskOrderPolicyStr)")
}

private object CliOptions {
    val tracePath: Option = Option.builder("i")
        .longOpt("trace-path")
        .hasArg()
        .required()
        .desc("path to trace file(s)")
        .build()

    val slackPath: Option = Option.builder("sd")
        .longOpt("slack-directory")
        .hasArg()
        .required()
        .desc("path to slack file(s)")
        .build()

    val traceFormat: Option = Option.builder("f")
            .longOpt("trace-format")
            .hasArg()
            .desc("format of the trace file(s)")
            .build()

    val resultDir: Option = Option.builder("o")
            .longOpt("result-dir")
            .hasArg()
            .desc("output directory for simulation results")
            .build()

    val machines: Option = Option.builder("m")
            .longOpt("machines")
            .hasArg()
            .desc("number of machines to simulate")
            .build()

    val cores: Option = Option.builder("c")
            .longOpt("cores")
            .hasArgs() // Note: not arg but argS -- multiple allowed.
            .desc("List of number of cores per simulated machine")
            .build()

    val dvfsEnabled: Option = Option.builder("e")
            .longOpt("dvfs")
            .hasArgs() // dvfs enabled per machine
            .desc("List of dynamic voltage scaling enabled per machine")
            .build()

    val TDPs: Option = Option.builder("t")
            .longOpt("tdp")
            .hasArgs() // dvfs enabled per machine
            .desc("List of TDP value per machine")
            .build()

    val baseClocks: Option = Option.builder("bc")
            .longOpt("base-clocks")
            .hasArgs() // dvfs enabled per machine
            .desc("List of base clock speed per machine")
            .build()

    val targetUtilization: Option = Option.builder("tu")
            .longOpt("target-utilization")
            .hasArg()
            .desc("target average utilization per machine")
            .build()

    val machineFractions: Option = Option.builder("mf")
            .longOpt("machine-fractions")
            .hasArgs()
            .desc("target fraction of the resource pool per machine")
            .build()

    val sampleFraction: Option = Option.builder()
            .longOpt("sample-fraction")
            .hasArg()
            .desc("fraction of tasks to sample from the trace")
            .build()

    val taskPlacementPolicy: Option = Option.builder()
            .longOpt("task-placement-policy")
            .hasArg()
            .desc("policy to use for placing tasks on machines")
            .build()

    val taskOrderPolicy: Option = Option.builder()
            .longOpt("task-order-policy")
            .hasArg()
            .desc("policy to use for determining the order to schedule tasks in")
            .build()

    val allOptions = Options().apply {
        addOption(tracePath)
        addOption(slackPath)
        addOption(traceFormat)
        addOption(sampleFraction)
        addOption(resultDir)
        addOption(machines)
        addOption(cores)
        addOption(dvfsEnabled)
        addOption(TDPs)
        addOption(baseClocks)
        addOption(targetUtilization)
        addOption(machineFractions)
        addOption(taskPlacementPolicy)
        addOption(taskOrderPolicy)
    }
}

private fun CommandLine.getOptionString(opt: Option): String {
    return this.getOptionValue(opt.longOpt)
}

private fun CommandLine.getOptionStringOrNull(opt: Option): String? {
    if (!this.hasOption(opt.longOpt)) return null
    return this.getOptionString(opt)
}

private fun CommandLine.getOptionInt(opt: Option): Int {
    val str = this.getOptionString(opt)
    return str.toIntOrNull() ?: throw ParseException("Value of option ${opt.longOpt} must be an integer")
}

private fun CommandLine.getOptionIntOrNull(opt: Option): Int? {
    if (!this.hasOption(opt.longOpt)) return null
    return this.getOptionInt(opt)
}

private fun CommandLine.getOptionBoolean(opt: Option): Boolean {
    if (!this.hasOption(opt.longOpt)) return false
    return this.getOptionString(opt).toLowerCase() == "true"
}

private fun CommandLine.getOptionDouble(opt: Option): Double {
    val str = this.getOptionString(opt)
    return str.toDoubleOrNull()
            ?: throw ParseException("Value of option ${opt.longOpt} must be a floating point number")
}

private fun CommandLine.getOptionDoubleOrNull(opt: Option): Double? {
    if (!this.hasOption(opt.longOpt)) return null
    return this.getOptionDouble(opt)
}

private fun CommandLine.getIntArrayOrNull(opt: Option): List<Int>? {
    if (!this.hasOption(opt.longOpt)) return null
    return this.getOptionValues(opt.longOpt).map { s -> parse<Int>(s) }
}

private fun CommandLine.getDoubleArrayOrNull(opt: Option): List<Double>? {
    if (!this.hasOption(opt.longOpt)) return null
    return this.getOptionValues(opt.longOpt).map { s -> parse<Double>(s) }
}

private fun CommandLine.getBooleanArrayOrNull(opt: Option): List<Boolean>? {
    if (!this.hasOption(opt.longOpt)) return null
    return this.getOptionValues(opt.longOpt).map { s -> parse<Boolean>(s) }
}


inline fun <reified T> parse(s: String) : T {
    return when (T::class) {
        Int::class -> s.toInt() as T
        Long::class -> s.toLong() as T
        Double::class -> s.toDouble() as T
        Float::class -> s.toFloat() as T
        Boolean::class -> s.toBoolean() as T
        String::class -> s as T
        // add other types here if need
        else -> throw IllegalStateException("Unknown Generic Type")
    }
}