import asyncio
import uuid
from sqlalchemy import select
from orion.config import settings
from orion.database import async_session
from orion.models.task import Task, TaskStatus
from orion.redis import redis

async def process_task(task_id:str):
    async with async_session() as db:
        result=await db.execute(select(Task).where(Task.id==uuid.UUID(task_id)))
        task=result.scalar_one_or_none()
        if not task:
            return
        task.status=TaskStatus.RUNNING
        await db.commit()
        await db.refresh(task)
        if task.task_type=="echo":
            task.status=TaskStatus.COMPLETED
            task.result=task.payload
        elif task.task_type=="sum":
            payload=task.payload
            if "numbers" in payload and isinstance(payload["numbers"], list) and all(isinstance(n, (int, float)) for n in payload["numbers"]):
                task.status=TaskStatus.COMPLETED
                task.result={"sum":sum(payload["numbers"])}
            else:
                task.status=TaskStatus.FAILED
                task.result={"error":"Payload must contain a list of numbers under 'numbers' key"}
        else:
            task.status=TaskStatus.FAILED
            task.result={"error":f"Unknown task type: {task.task_type}"}
        await db.commit()

async def main():
    while True:
        res=await redis.blpop(settings.queue_name, timeout=0)
        if res:
            _,task_id=res
            await process_task(task_id)

if __name__=="__main__":
    asyncio.run(main())
