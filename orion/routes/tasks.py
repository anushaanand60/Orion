import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from orion.config import settings
from orion.database import get_db
from orion.models.task import Task
from orion.redis import redis
from orion.schemas.task import TaskCreate, TaskDetailResponse, TaskResponse

router=APIRouter()

@router.post("/tasks", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
async def create_task(task_in:TaskCreate, db:AsyncSession=Depends(get_db)):
    task=Task(
        id=uuid.uuid4(),
        status="pending",
        task_type=task_in.task_type,
        payload=task_in.payload,
        max_retries=task_in.max_retries,
        result=None
    )
    db.add(task)
    await db.commit()
    await db.refresh(task)
    await redis.rpush(settings.queue_name, str(task.id))
    return task

@router.get("/tasks/{task_id}", response_model=TaskDetailResponse)
async def get_task(task_id:uuid.UUID, db:AsyncSession=Depends(get_db)):
    result=await db.execute(select(Task).where(Task.id==task_id))
    task=result.scalar_one_or_none()
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found"
        )
    return task
