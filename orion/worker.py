import asyncio
import uuid
from sqlalchemy import select
from orion.config import settings
from orion.database import async_session
from orion.models.task import Task
from orion.redis import redis

async def process_task(task_id:str):
    async with async_session() as db:
        result=await db.execute(select(Task).where(Task.id==uuid.UUID(task_id)))
        task=result.scalar_one_or_none()
        if not task:
            return
        task.status="running"
        await db.commit()
        await db.refresh(task)
        task.status="completed"
        task.result={"message":"Task completed"}
        await db.commit()

async def main():
    while True:
        res=await redis.blpop(settings.queue_name, timeout=0)
        if res:
            _,task_id=res
            await process_task(task_id)

if __name__=="__main__":
    asyncio.run(main())
