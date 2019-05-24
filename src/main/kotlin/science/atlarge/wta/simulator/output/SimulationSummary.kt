package science.atlarge.wta.simulator.output

import org.apache.commons.math3.stat.descriptive.moment.GeometricMean
import org.apache.commons.math3.stat.descriptive.moment.Mean
import org.apache.commons.math3.stat.descriptive.moment.Variance
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.commons.math3.util.FastMath
import science.atlarge.wta.simulator.model.Trace
import java.io.File
import java.io.StringWriter
import java.io.Writer

class SimulationSummary(
        private val trace: Trace,
        private val taskStats: TaskStatsCollector,
        private val workflowStats: WorkflowStatsCollector
) {

    private fun computePercentiles(values: DoubleArray): List<Double> {
        val percentileComputation = Percentile().also { it.data = values }
        return PERCENTILES.map { percentile ->
            if (percentile == 0.0) values[0]
            else percentileComputation.evaluate(percentile)
        }
    }

    private fun computeHarmonicMean(values: DoubleArray): Double {
        val invValues = DoubleArray(values.size) { i ->
            1.0 / values[i]
        }
        val invMean = Mean().evaluate(invValues)
        return 1.0 / invMean
    }

    private fun computeStats(values: DoubleArray): Stats {
        val sortedValues = values.clone().also { it.sort() }
        val count = sortedValues.size
        val mean = Mean().evaluate(sortedValues)
        val variance = Variance().evaluate(sortedValues, mean)
        val geometricMean = GeometricMean().evaluate(sortedValues)
        val harmonicMean = computeHarmonicMean(sortedValues)
        val percentiles = computePercentiles(sortedValues)
        return Stats(count, mean, FastMath.sqrt(variance), variance / mean, geometricMean, harmonicMean, percentiles)
    }

    private fun extractTaskRunTimes(): DoubleArray {
        return DoubleArray(trace.tasks.size) { i -> trace.tasks[i].runTime.toDouble() }
    }

    private fun extractTaskWaitTimes(): DoubleArray {
        return DoubleArray(trace.tasks.size) { i ->
            val task = trace.tasks[i]
            (taskStats.startTimeOf(task) - taskStats.readyTimeOf(task)).toDouble()
        }
    }

    private fun extractTaskResponseTimes(taskRunTimes: DoubleArray, taskWaitTimes: DoubleArray): DoubleArray {
        return DoubleArray(trace.tasks.size) { i -> taskRunTimes[i] + taskWaitTimes[i] }
    }

    private fun extractTaskBoundedSlowdown(taskRunTimes: DoubleArray, taskResponseTimes: DoubleArray, bound: Double): DoubleArray {
        return DoubleArray(trace.tasks.size) { i -> maxOf(1.0, taskResponseTimes[i] / maxOf(taskRunTimes[i], bound)) }
    }

    private fun extractWorkflowScheduleLengths(): DoubleArray {
        return DoubleArray(trace.workflows.size) { i ->
            val workflow = trace.workflows[i]
            (workflowStats.completionTimeOf(workflow) - workflowStats.submitTimeOf(workflow)).toDouble()
        }
    }

    private fun extractWorkflowNormalizedScheduleLengths(workflowScheduleLengths: DoubleArray): DoubleArray {
        val normalizedScheduleLengths = mutableListOf<Double>()
        trace.workflows.forEachIndexed { i, workflow ->
            val minimalScheduleLength = (workflowStats.earliestCompletionTimeOf(workflow) -
                    workflowStats.submitTimeOf(workflow)).toDouble()
            if (minimalScheduleLength > 0.0) {
                normalizedScheduleLengths.add(workflowScheduleLengths[i] / minimalScheduleLength)
            }
        }
        return normalizedScheduleLengths.toDoubleArray()
    }

    private fun writeSummary(writer: Writer) {
        writer.appendln("metric\tcount\tmean\tstd.dev\tcv\tgeo.mean\tharm.mean\t${
        PERCENTILE_LABELS.joinToString("\t")}")

        fun f(value: Double): String {
            return String.format("%.2f", value)
        }

        fun writeMetric(metric: String, values: DoubleArray) {
            val stats = computeStats(values)
            writer.append(metric).append('\t')
                    .append(stats.count.toString()).append('\t')
                    .append(f(stats.mean)).append('\t')
                    .append(f(stats.standardDeviation)).append('\t')
                    .append(f(stats.coefficientOfVariation)).append('\t')
                    .append(f(stats.geometricMean)).append('\t')
                    .append(f(stats.harmonicMean))
            for (p in stats.percentiles) {
                writer.append('\t').append(f(p))
            }
            writer.appendln()
        }

        // Write summary statistics for various metrics:
        // - Task run times (= completion time - start time)
        val taskRunTimes = extractTaskRunTimes()
        writeMetric("Task Run Time", taskRunTimes)
        // - Task wait times (= start time - ready time)
        val taskWaitTimes = extractTaskWaitTimes()
        writeMetric("Task Wait Time", taskWaitTimes)
        // - Task response time (= run time + wait time)
        val taskResponseTimes = extractTaskResponseTimes(taskRunTimes, taskWaitTimes)
        writeMetric("Task Response Time", taskResponseTimes)
        // - Task bounded slowdown (= max{response time / max{run time, lower bound}, 1})
        //   Computed for various bounds
        for (e in 0..9) {
            val boundedSlowdowns = extractTaskBoundedSlowdown(taskRunTimes, taskResponseTimes,
                    Math.pow(10.0, e.toDouble()))
            writeMetric("Task Bounded Slowdown (1e$e Ticks)", boundedSlowdowns)
        }

        // - Workflow schedule length (= completion time - start time)
        val workflowScheduleLengths = extractWorkflowScheduleLengths()
        writeMetric("Workflow Schedule Length", workflowScheduleLengths)
        // - Workflow normalized schedule length (= critical path length / minimal critical path length)
        val workflowNormalizedScheduleLengths = extractWorkflowNormalizedScheduleLengths(workflowScheduleLengths)
        writeMetric("Workflow Normalized Schedule Length", workflowNormalizedScheduleLengths)
    }

    fun writeToFile(outputFile: File) {
        outputFile.bufferedWriter().use(this::writeSummary)
    }

    fun writeToTerminal() {
        val writer = StringWriter()
        writeSummary(writer)
        println(writer.toString())
    }

    companion object {
        private val PERCENTILES = doubleArrayOf(0.0, 25.0, 50.0, 75.0, 95.0, 99.0, 100.0)
        private val PERCENTILE_LABELS = arrayOf("min", "p25", "median", "p75", "p95", "p99", "max")
    }

}

private data class Stats(
        val count: Int,
        val mean: Double,
        val standardDeviation: Double,
        val coefficientOfVariation: Double,
        val geometricMean: Double,
        val harmonicMean: Double,
        val percentiles: List<Double>
)