import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict
from orion.enums import TaskStatus, TaskPriority

class TaskCreate(BaseModel):
    task_type:str
    payload:Dict[str, Any]
    max_retries:Optional[int]=3
    priority:Optional[TaskPriority]=TaskPriority.DEFAULT
    scheduled_at:Optional[datetime]=None
    dependencies:Optional[List[uuid.UUID]]=None

class TaskResponse(BaseModel):
    id:uuid.UUID
    status:TaskStatus
    priority:TaskPriority
    model_config=ConfigDict(from_attributes=True)

class TaskDetailResponse(TaskResponse):
    task_type:str
    payload:Dict[str, Any]
    result:Optional[Dict[str, Any]]
    retry_count:int
    max_retries:int
    scheduled_at:Optional[datetime]
    dependencies:List[uuid.UUID]
    created_at:datetime
    updated_at:datetime
