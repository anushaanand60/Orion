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
