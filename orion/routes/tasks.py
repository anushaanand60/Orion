import uuid
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from orion.config import settings
from orion.database import get_db
from orion.queue import get_queue_name
from orion.models.task import Task
from orion.enums import TaskStatus, TaskEventType
from orion.events import record_event
from orion.redis import redis
from orion.schemas.task import TaskCreate, TaskDetailResponse, TaskResponse

router=APIRouter()

@router.get("/dead-letter", response_model=List[TaskDetailResponse], tags=["Dead Letter"])
async def get_dead_letter_tasks(db:AsyncSession=Depends(get_db)):
    task_ids=await redis.lrange(settings.dead_letter_queue_name, 0, -1)
    tasks=[]
    for task_id in task_ids:
        result=await db.execute(
            select(Task)
            .options(selectinload(Task.parents))
            .where(Task.id==uuid.UUID(task_id))
        )
        task=result.scalar_one_or_none()
        if task:
            tasks.append(task)
    return tasks

@router.post("/tasks", response_model=TaskResponse, status_code=status.HTTP_201_CREATED, tags=["Tasks"])
async def create_task(task_in:TaskCreate, db:AsyncSession=Depends(get_db)):
    parents=[]
    if task_in.dependencies:
        for parent_id in task_in.dependencies:
            result=await db.execute(select(Task).where(Task.id==parent_id))
            parent=result.scalar_one_or_none()
            if not parent:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Dependency parent task {parent_id} not found"
                )
            parents.append(parent)

    status_val=TaskStatus.BLOCKED if parents else TaskStatus.PENDING
    if parents and all(parent.status==TaskStatus.COMPLETED for parent in parents):
        status_val=TaskStatus.PENDING

    task=Task(
        id=uuid.uuid4(),
        status=status_val,
        task_type=task_in.task_type,
        payload=task_in.payload,
        max_retries=task_in.max_retries,
        priority=task_in.priority,
        scheduled_at=task_in.scheduled_at,
        parents=parents,
        result=None
    )
    db.add(task)
    await record_event(db, task.id, TaskEventType.TASK_CREATED)
    if status_val==TaskStatus.BLOCKED:
        await record_event(db, task.id, TaskEventType.TASK_BLOCKED)
    else:
        if task.scheduled_at is None:
            await record_event(db, task.id, TaskEventType.TASK_ENQUEUED)
        else:
            await record_event(db, task.id, TaskEventType.TASK_SCHEDULED)

    await db.commit()
    await db.refresh(task)
    await db.refresh(task, ["parents"])

    if status_val==TaskStatus.PENDING:
        if task.scheduled_at is None:
            await redis.rpush(get_queue_name(task.priority), str(task.id))
        else:
            await redis.zadd(settings.scheduled_queue_name, {str(task.id): task.scheduled_at.timestamp()})
    return task

@router.get("/tasks/{task_id}", response_model=TaskDetailResponse, tags=["Tasks"])
async def get_task(task_id:uuid.UUID, db:AsyncSession=Depends(get_db)):
    result=await db.execute(
        select(Task)
        .options(selectinload(Task.parents))
        .where(Task.id==task_id)
    )
    task=result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
    return task
