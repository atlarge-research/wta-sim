package science.atlarge.wta.simulator.input

import science.atlarge.wta.simulator.model.Trace
import java.nio.file.Path

abstract class TraceReader {

    fun readTraceFromPaths(vararg path: Path, slackDirectory: Path): Trace {
        return readTraceFromPaths(path.asIterable(), slackDirectory)
    }

    abstract fun readTraceFromPaths(paths: Iterable<Path>, slackDirectory: Path): Trace

}