import uuid
from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from orion.database import get_db
from orion.models.task import Task
from orion.schemas.task import TaskCreate, TaskResponse

router=APIRouter()

@router.post("/tasks", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
async def create_task(task_in:TaskCreate, db:AsyncSession=Depends(get_db)):
    task=Task(
        id=uuid.uuid4(),
        status="pending",
        payload=task_in.payload,
        result=None
    )
    db.add(task)
    await db.commit()
    await db.refresh(task)
    return TaskResponse(id=task.id, status=task.status)
