import asyncio
from datetime import datetime, timezone
from sqlalchemy import select, and_
from orion.config import settings
from orion.database import async_session
from orion.enums import TaskStatus
from orion.models.task import Task
from orion.queue import get_queue_name
from orion.redis import redis

async def recover_expired_leases():
    async with async_session() as db:
        result=await db.execute(
            select(Task).where(
                and_(
                    Task.status==TaskStatus.RUNNING,
                    Task.worker_id.isnot(None),
                    Task.lease_expires_at.isnot(None),
                    Task.lease_expires_at<datetime.now(timezone.utc),
                )
            )
        )
        expired_tasks=result.scalars().all()
        for task in expired_tasks:
            print(f"Recovering task {task.id} from worker {task.worker_id}")
            task.status=TaskStatus.PENDING
            task.worker_id=None
            task.lease_expires_at=None
            await db.commit()
            await redis.rpush(get_queue_name(task.priority), str(task.id))

async def main():
    print("Recovery process starting...")
    while True:
        await recover_expired_leases()
        await asyncio.sleep(settings.lease_recovery_interval)

if __name__=="__main__":
    asyncio.run(main())
