from enum import Enum

class TaskStatus(str, Enum):
    PENDING="pending"
    RUNNING="running"
    COMPLETED="completed"
    FAILED="failed"
    BLOCKED="blocked"

class TaskPriority(str, Enum):
    HIGH="high"
    DEFAULT="default"
    LOW="low"

class TaskEventType(str, Enum):
    TASK_CREATED="task_created"
    TASK_ENQUEUED="task_enqueued"
    TASK_STARTED="task_started"
    TASK_COMPLETED="task_completed"
    TASK_FAILED="task_failed"
    TASK_BLOCKED="blocked"
    TASK_UNBLOCKED="unblocked"
    TASK_SCHEDULED="scheduled"
    TASK_PROMOTED="promoted"
    LEASE_ACQUIRED="lease_acquired"
    LEASE_EXPIRED="lease_expired"
    TASK_RECOVERED="recovered"
    RETRY_SCHEDULED="retry_scheduled"
    DLQ_PUSHED="dlq_pushed"
