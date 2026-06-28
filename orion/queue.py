from orion.config import settings
from orion.enums import TaskPriority

PRIORITY_QUEUE_MAP={
    TaskPriority.HIGH:settings.high_queue_name,
    TaskPriority.DEFAULT:settings.default_queue_name,
    TaskPriority.LOW:settings.low_queue_name,
}

def get_queue_name(priority:TaskPriority)->str:
    return PRIORITY_QUEUE_MAP.get(priority, settings.default_queue_name)
