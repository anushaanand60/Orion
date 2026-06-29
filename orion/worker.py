import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from orion.config import settings
from orion.database import async_session
from orion.enums import TaskStatus
from orion.exceptions import PermanentTaskError, RetryableTaskError
from orion.models.task import Task
from orion.queue import get_queue_name
from orion.redis import redis

async def release_blocked_tasks(db, task_id:uuid.UUID):
    result=await db.execute(
        select(Task)
        .options(selectinload(Task.children).selectinload(Task.parents))
        .where(Task.id==task_id)
    )
    completed_task=result.scalar_one_or_none()
    if not completed_task:
        return
    now=datetime.now(timezone.utc)
    for child in completed_task.children:
        if child.status!=TaskStatus.BLOCKED:
            continue
        all_completed=all(parent.status==TaskStatus.COMPLETED for parent in child.parents)
        if all_completed:
            print(f"Releasing child task {child.id} from blocked state")
            child.status=TaskStatus.PENDING
            await db.commit()
            if child.scheduled_at is None:
                await redis.rpush(get_queue_name(child.priority), str(child.id))
            elif child.scheduled_at<=now:
                await redis.rpush(get_queue_name(child.priority), str(child.id))
            else:
                await redis.zadd(settings.scheduled_queue_name, {str(child.id): child.scheduled_at.timestamp()})

async def process_task(task_id:str):
    async with async_session() as db:
        result=await db.execute(select(Task).where(Task.id==uuid.UUID(task_id)))
        task=result.scalar_one_or_none()
        if not task:
            return
        task.status=TaskStatus.RUNNING
        task.worker_id=settings.worker_name
        task.lease_expires_at=datetime.now(timezone.utc)+timedelta(seconds=settings.worker_lease_seconds)
        await db.commit()
        try:
            if task.task_type=="echo":
                task.status=TaskStatus.COMPLETED
                task.result=task.payload
            elif task.task_type=="slow_echo":
                delay=task.payload.get("delay", 1)
                await asyncio.sleep(delay)
                task.status=TaskStatus.COMPLETED
                task.result=task.payload
            elif task.task_type=="sum":
                numbers=task.payload.get("numbers")
                if isinstance(numbers, list) and all(isinstance(n, (int, float)) for n in numbers):
                    task.status=TaskStatus.COMPLETED
                    task.result={"sum":sum(numbers)}
                else:
                    raise PermanentTaskError("Payload must contain a list of numbers under 'numbers' key")
            elif task.task_type=="fail":
                delay=task.payload.get("delay", 0)
                if delay>0:
                    await asyncio.sleep(delay)
                raise RetryableTaskError("Simulated failure")
            else:
                raise PermanentTaskError(f"Unknown task type: {task.task_type}")
            task.worker_id=None
            task.lease_expires_at=None
            await db.commit()
            await release_blocked_tasks(db, task.id)
        except PermanentTaskError as e:
            task.status=TaskStatus.FAILED
            task.result={"error":str(e)}
            task.worker_id=None
            task.lease_expires_at=None
            await db.commit()
            await redis.rpush(settings.dead_letter_queue_name, str(task.id))
        except Exception as e:
            task.retry_count+=1
            if task.retry_count<=task.max_retries:
                task.status=TaskStatus.PENDING
                task.worker_id=None
                task.lease_expires_at=None
                await db.commit()
                await redis.rpush(get_queue_name(task.priority), str(task.id))
            else:
                task.status=TaskStatus.FAILED
                task.result={"error":str(e)}
                task.worker_id=None
                task.lease_expires_at=None
                await db.commit()
                await redis.rpush(settings.dead_letter_queue_name, str(task.id))

async def main():
    print(f"Worker {settings.worker_name} starting...")
    while True:
        res=await redis.blpop([settings.high_queue_name, settings.default_queue_name, settings.low_queue_name], timeout=0)
        if res:
            _,task_id=res
            print(f"Worker {settings.worker_name} processing task {task_id}")
            await process_task(task_id)

if __name__=="__main__":
    asyncio.run(main())
