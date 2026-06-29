import asyncio
import uuid
from datetime import datetime, timezone
from sqlalchemy import select
from orion.config import settings
from orion.database import async_session
from orion.models.task import Task
from orion.queue import get_queue_name
from orion.redis import redis
from orion.enums import TaskEventType
from orion.events import record_event

async def promote_ready_tasks():
    now=datetime.now(timezone.utc)
    task_ids=await redis.zrangebyscore(settings.scheduled_queue_name, "-inf", now.timestamp())
    if not task_ids:
        return
    await redis.zrem(settings.scheduled_queue_name, *task_ids)
    async with async_session() as db:
        for task_id in task_ids:
            result=await db.execute(select(Task).where(Task.id==uuid.UUID(task_id)))
            task=result.scalar_one_or_none()
            if task:
                print(f"Promoting task {task.id} to queue {get_queue_name(task.priority)}")
                await record_event(db, task.id, TaskEventType.TASK_PROMOTED)
                await record_event(db, task.id, TaskEventType.TASK_ENQUEUED)
                await db.commit()
                await redis.rpush(get_queue_name(task.priority), str(task.id))

async def main():
    print("Scheduler process starting...")
    while True:
        try:
            await promote_ready_tasks()
        except Exception as e:
            print(f"Scheduler error: {e}")
        await asyncio.sleep(settings.scheduler_poll_interval)

if __name__=="__main__":
    asyncio.run(main())
