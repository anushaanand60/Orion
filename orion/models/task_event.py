import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from sqlalchemy import DateTime, ForeignKey, JSON, String, Enum as SQLEnum
from sqlalchemy.orm import Mapped, mapped_column, relationship
from orion.models.base import Base
from orion.enums import TaskEventType

class TaskEvent(Base):
    __tablename__="task_events"
    id:Mapped[uuid.UUID]=mapped_column(primary_key=True, default=uuid.uuid4)
    task_id:Mapped[uuid.UUID]=mapped_column(ForeignKey("tasks.id", ondelete="CASCADE"), nullable=False)
    timestamp:Mapped[datetime]=mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    event_type:Mapped[TaskEventType]=mapped_column(SQLEnum(TaskEventType, native_enum=False), nullable=False)
    worker_id:Mapped[Optional[str]]=mapped_column(String(100), nullable=True)
    details:Mapped[Optional[Dict[str, Any]]]=mapped_column(type_=JSON, nullable=True)

    task:Mapped["Task"]=relationship("Task", back_populates="events")
