import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel

class TaskCreate(BaseModel):
    payload:Dict[str, Any]

class TaskResponse(BaseModel):
    id:uuid.UUID
    status:str

class TaskDetailResponse(TaskResponse):
    payload:Dict[str, Any]
    result:Optional[Dict[str, Any]]
    created_at:datetime
    updated_at:datetime

