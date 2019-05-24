package science.atlarge.wta.simulator.allocation

class ShortestJobFirstOrder : TaskOrderPolicy {

    override fun compareTasks(task1: QueuedTask, task2: QueuedTask): Int {
        return when {
            task1.task.runTime < task2.task.runTime -> -1
            task1.task.runTime > task2.task.runTime -> 1
            task1.submissionTime < task2.submissionTime -> -1
            task1.submissionTime > task2.submissionTime -> 1
            task1.task.id < task2.task.id -> -1
            task1.task.id > task2.task.id -> 1
            else -> 0
        }
    }

}