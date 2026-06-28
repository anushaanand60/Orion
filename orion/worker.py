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
        try:
            if task.task_type=="echo":
                task.status=TaskStatus.COMPLETED
                task.result=task.payload
            elif task.task_type=="sum":
                numbers=task.payload.get("numbers")
                if isinstance(numbers, list) and all(isinstance(n, (int, float)) for n in numbers):
                    task.status=TaskStatus.COMPLETED
                    task.result={"sum":sum(numbers)}
                else:
                    raise ValueError("Payload must contain a list of numbers under 'numbers' key")
            elif task.task_type=="fail":
                raise RuntimeError("Simulated failure")
            else:
                raise ValueError(f"Unknown task type: {task.task_type}")
            await db.commit()
        except Exception as e:
            task.retry_count+=1
            if task.retry_count<=task.max_retries:
                task.status=TaskStatus.PENDING
                await db.commit()
                await redis.rpush(settings.queue_name, str(task.id))
            else:
                task.status=TaskStatus.FAILED
                task.result={"error":str(e)}
                await db.commit()

async def main():
    while True:
        res=await redis.blpop(settings.queue_name, timeout=0)
        if res:
            _,task_id=res
            await process_task(task_id)

if __name__=="__main__":
    asyncio.run(main())
