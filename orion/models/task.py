import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from sqlalchemy import DateTime, JSON, func
from sqlalchemy.orm import Mapped, mapped_column
from orion.models.base import Base

class Task(Base):
    __tablename__="tasks"
    id:Mapped[uuid.UUID]=mapped_column(primary_key=True, default=uuid.uuid4)
    status:Mapped[str]=mapped_column(default="pending")
    payload:Mapped[Dict[str, Any]]=mapped_column(type_=JSON)
    result:Mapped[Optional[Dict[str, Any]]]=mapped_column(type_=JSON, nullable=True)
    created_at:Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at:Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
