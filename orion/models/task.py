import uuid
from datetime import datetime
from typing import Any, Dict, Optional, TYPE_CHECKING
from sqlalchemy import DateTime, Enum as SQLEnum, JSON, func, Table, Column, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from orion.models.base import Base
from orion.enums import TaskStatus, TaskPriority

if TYPE_CHECKING:
    from orion.models.task_event import TaskEvent

task_dependencies=Table(
    "task_dependencies",
    Base.metadata,
    Column("parent_task_id", ForeignKey("tasks.id", ondelete="CASCADE"), primary_key=True),
    Column("child_task_id", ForeignKey("tasks.id", ondelete="CASCADE"), primary_key=True),
)

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
    worker_id:Mapped[Optional[str]]=mapped_column(nullable=True)
    lease_expires_at:Mapped[Optional[datetime]]=mapped_column(DateTime(timezone=True), nullable=True)
    scheduled_at:Mapped[Optional[datetime]]=mapped_column(DateTime(timezone=True), nullable=True)
    created_at:Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at:Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    parents:Mapped[list["Task"]]=relationship(
        "Task",
        secondary=task_dependencies,
        primaryjoin="Task.id==task_dependencies.c.child_task_id",
        secondaryjoin="Task.id==task_dependencies.c.parent_task_id",
        back_populates="children",
    )
    children:Mapped[list["Task"]]=relationship(
        "Task",
        secondary=task_dependencies,
        primaryjoin="Task.id==task_dependencies.c.parent_task_id",
        secondaryjoin="Task.id==task_dependencies.c.child_task_id",
        back_populates="parents",
    )

    events:Mapped[list["TaskEvent"]]=relationship(
        "TaskEvent",
        back_populates="task",
        order_by="TaskEvent.timestamp.asc()",
        cascade="all, delete-orphan"
    )

    @property
    def dependencies(self) -> list[uuid.UUID]:
        return [parent.id for parent in self.parents]
