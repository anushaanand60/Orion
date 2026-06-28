import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, ConfigDict
from orion.models.task import TaskStatus

class TaskCreate(BaseModel):
    task_type:str
    payload:Dict[str, Any]

class TaskResponse(BaseModel):
    id:uuid.UUID
    status:TaskStatus
    model_config=ConfigDict(from_attributes=True)

class TaskDetailResponse(TaskResponse):
    task_type:str
    payload:Dict[str, Any]
    result:Optional[Dict[str, Any]]
    created_at:datetime
    updated_at:datetime
