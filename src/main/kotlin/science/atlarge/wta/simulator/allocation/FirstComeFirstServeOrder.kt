package science.atlarge.wta.simulator.allocation

class FirstComeFirstServeOrder : TaskOrderPolicy {

    override fun compareTasks(task1: QueuedTask, task2: QueuedTask): Int {
        return when {
            task1.submissionTime < task2.submissionTime -> -1
            task1.submissionTime > task2.submissionTime -> 1
            task1.task.id < task2.task.id -> -1
            task1.task.id > task2.task.id -> 1
            else -> 0
        }
    }

}