from enum import Enum

class TaskStatus(str, Enum):
    PENDING="pending"
    RUNNING="running"
    COMPLETED="completed"
    FAILED="failed"

class TaskPriority(str, Enum):
    HIGH="high"
    DEFAULT="default"
    LOW="low"
