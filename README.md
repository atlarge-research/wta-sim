# Simulator for analyzing traces from the Workflow Trace Archive

The WTA Simulator (WTASim) can be used to replay WTA traces in virtual datacenters with a variety of schedulers.

The input of the simulator consists of a WTA trace, a description of the simulated environment, and a set of scheduling policies.
The simulator outputs basic metrics for every task and workflow in the trace (e.g., response time, slowdown), as well as distributions of each metric.

## How to use the WTASim?

Follow these steps to obtain the simulator:

1. Install [Maven](https://maven.apache.org/)
2. Download the WTASim sources by cloning this repository or downloading a source archive [here](https://github.com/lfdversluis/wta-sim/archive/master.zip)
3. Inside the `wta-sim` root directory, compile the simulator: `mvn package`

The simulator is now available as a JAR file in the `wta-sim/target` directory (e.g., `wta-sim/target/wta-sim-0.1.jar` for the version 0.1 release).
To run a first simulation, download a trace from the Workflow Trace Archive to simulate and run the following command:

```bash
java -cp target/wta-sim-0.1.jar science.atlarge.wta.simulator.WTASim \
  -i /path/to/trace/${trace_name}_parquet \
  -o simulation_results/${trace_name} \
  --target-utilization 0.7
```

replacing `${trace_name}` with the name of the trace you want to simulate.

## Description of command-line options

The simulation process can be controlled though various command-line options described in more detail in this section.

### Configuring input/output of the simulator

`--trace-path` (`-i`): Path of the trace data to read and simulate. Required.

`--trace-format` (`-f`): Format of the trace data.
Defaults to `wta`, indicating the trace is formatted according to the Workload Trace Archive format.
Support for other formats may be added in the future.

Note: For WTA traces, the specified trace path should be the root directory of the trace data
(i.e., there should be a `workflows` and a `tasks` directory inside the directory passed to the `-i` option).

`--sample-fraction`: Fraction of tasks to sample from the input trace. Defaults to `1` (all tasks).
When specified, WTASim randomly selects a subset of workflows from the input trace such that the total number of tasks
in the selected workflows is approximately the given fraction of the total number of tasks in the trace.
Note that this method is not guaranteed to preserve any characteristics of the original trace.
This value should be a fraction between `0` (exclusive) and `1` (inclusive). For example, a value of `0.1` results in
running the simulation on approximately 10% of the input trace.

`--result-dir` (`-o`): Output directory to store simulation results in.
If set, WTASim produces three files in the specified directory: `tasks.tsv` containing task-level metrics,
`workflows.tsv` containing workflow-level metrics, and `summary.tsv` containing aggregate metrics and distributions.

### Configuring the simulated environment

`--machines` (`-m`): The number of machines to simulate.

`--cores` (`-c`): The number of cores per machine to simulate. Defaults to `1`.
If the largest task in the input trace requires more cores than the specified number,
WTASim will override this option to be equal to the number of cores required by the largest task.
This ensures that the input trace can be replayed in full.

`--target-utilization`: Instead of directly specifying the number of machines to simulate,
WTASim can compute how many machines are needed to reach an average utilization of the simulated environment
approximately equal to the specified target. This value should be a fraction between `0` (exclusive) and `1` (inclusive).
For example, a value of `0.7` results in a simulated environment with approximately 70% average utilization.

Note: Exactly one of the `--machines` and `--target-utilization` options must be specified
to define the size of the simulated environment.

### Configuring the simulated scheduler

`--task-order-policy`: The scheduling policy to use for determining in which order to (try to) schedule tasks.
Supported values for this option and the associated policies are:

- `fcfs` (default): First-Come First-Serve, schedules tasks in the order they are submitted to the scheduler.
- `sjf`: Shortest Job First, schedules tasks in ascending order of runtime.
- `ewf`: Earliest Workflow First, schedules workflows in FCFS order. Tasks belonging to the same workflow are also scheduled in FCFS order.

`--task-placement-policy`: The scheduling policy to use for determining which machine to allocate a task to.
Supported values for this option and the associated policies are:

- `best_fit` (default): Best-Fit placement with backfilling, allocates a task to the machine with the least number of available
resources among all machines with sufficient resources to run the given task. If no machines with sufficient resources
are available, the scheduler will attempt to allocate the next task in the queue (backfilling).

## Reproducing the simulation experiments of our SC19 submission

The following command was used for each simulation in our SC19 submission:

```bash
java -cp target/wta-sim-0.1.jar science.atlarge.wta.simulator.WTASim \
  -i /path/to/trace/${trace_name}_parquet \
  -o simulation_results/${trace_name}/${policy} \
  --target-utilization 0.7 \
  --task-placement-policy best_fit \
  --task-order-policy ${policy}
```

where `${trace_name}` and `${policy}` were replaced with the name of the trace to be analyzed and the task order policy
to be used, respectively.
