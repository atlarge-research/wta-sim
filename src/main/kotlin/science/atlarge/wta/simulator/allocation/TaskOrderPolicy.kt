package science.atlarge.wta.simulator.allocation

import science.atlarge.wta.simulator.model.Task
import science.atlarge.wta.simulator.model.Ticks

interface TaskOrderPolicy : Comparator<QueuedTask> {

    override fun compare(o1: QueuedTask?, o2: QueuedTask?): Int {
        // Throw NPEs if either argument is null, as per the contract of Comparator::compare
        return compareTasks(o1!!, o2!!)
    }

    fun compareTasks(task1: QueuedTask, task2: QueuedTask): Int

}

data class QueuedTask(
        val task: Task,
        val submissionTime: Ticks
)
