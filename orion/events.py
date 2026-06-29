import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from orion.models.task_event import TaskEvent
from orion.enums import TaskEventType

async def record_event(
    db:AsyncSession,
    task_id:uuid.UUID,
    event_type:TaskEventType,
    worker_id:Optional[str]=None,
    details:Optional[Dict[str, Any]]=None
):
    event=TaskEvent(
        id=uuid.uuid4(),
        task_id=task_id,
        timestamp=datetime.now(timezone.utc),
        event_type=event_type,
        worker_id=worker_id,
        details=details
    )
    db.add(event)
