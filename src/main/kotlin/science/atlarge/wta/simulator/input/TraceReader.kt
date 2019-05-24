package science.atlarge.wta.simulator.input

import science.atlarge.wta.simulator.model.Trace
import java.nio.file.Path

abstract class TraceReader {

    fun readTraceFromPaths(vararg path: Path): Trace {
        return readTraceFromPaths(path.asIterable())
    }

    abstract fun readTraceFromPaths(paths: Iterable<Path>): Trace

}