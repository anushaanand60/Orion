class TaskExecutionError(Exception):
    pass

class RetryableTaskError(TaskExecutionError):
    pass

class PermanentTaskError(TaskExecutionError):
    pass
