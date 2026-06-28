import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from sqlalchemy import DateTime, Enum as SQLEnum, JSON, func
from sqlalchemy.orm import Mapped, mapped_column
from orion.models.base import Base
from orion.enums import TaskStatus, TaskPriority

class Task(Base):
    __tablename__="tasks"
    id:Mapped[uuid.UUID]=mapped_column(primary_key=True, default=uuid.uuid4)
    status:Mapped[TaskStatus]=mapped_column(SQLEnum(TaskStatus, native_enum=False), default=TaskStatus.PENDING)
    priority:Mapped[TaskPriority]=mapped_column(SQLEnum(TaskPriority, native_enum=False), default=TaskPriority.DEFAULT, server_default="default")
    task_type:Mapped[str]=mapped_column()
    retry_count:Mapped[int]=mapped_column(default=0, server_default="0")
    max_retries:Mapped[int]=mapped_column(default=3, server_default="3")
    payload:Mapped[Dict[str, Any]]=mapped_column(type_=JSON)
    result:Mapped[Optional[Dict[str, Any]]]=mapped_column(type_=JSON, nullable=True)
    created_at:Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at:Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
