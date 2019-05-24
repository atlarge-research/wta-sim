package science.atlarge.wta.simulator.allocation

import science.atlarge.wta.simulator.output.WorkflowStatsCollector

class EarliestWorkflowFirstOrder : TaskOrderPolicy {

    private val fcfs = FirstComeFirstServeOrder()
    lateinit var workflowStats: WorkflowStatsCollector

    override fun compareTasks(task1: QueuedTask, task2: QueuedTask): Int {
        val wf1 = task1.task.workflow
        val wf2 = task2.task.workflow
        return when {
            wf1 != null && wf2 == null -> -1
            wf1 == null && wf2 != null -> 1
            wf1 != null && wf2 != null && wf1 !== wf2 -> {
                val wf1Time = workflowStats.submitTimeOf(wf1)
                val wf2Time = workflowStats.submitTimeOf(wf2)
                when {
                    wf1Time < wf2Time -> -1
                    wf1Time > wf2Time -> 1
                    wf1.id < wf2.id -> -1
                    wf1.id > wf2.id -> 1
                    else -> throw IllegalArgumentException("$wf1 !== $wf2, but IDs are the same")
                }
            }
            else -> fcfs.compareTasks(task1, task2)
        }
    }

}