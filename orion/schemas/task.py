import uuid
from typing import Any, Dict
from pydantic import BaseModel

class TaskCreate(BaseModel):
    payload:Dict[str, Any]

class TaskResponse(BaseModel):
    id:uuid.UUID
    status:str
