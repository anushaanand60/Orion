import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from sqlalchemy import select, func, text, and_, cast, String
from sqlalchemy.orm import selectinload
from orion.config import settings
from orion.database import async_session
from orion.models.task import Task
from orion.models.task_event import TaskEvent
from orion.enums import TaskStatus, TaskEventType
from orion.redis import redis

def to_ist_str(dt:Optional[datetime]) -> str:
    if dt is None:
        return "-"
    if dt.tzinfo is None:
        dt=dt.replace(tzinfo=timezone.utc)
    ist_zone=timezone(timedelta(hours=5, minutes=30))
    return dt.astimezone(ist_zone).strftime("%d %b %Y, %H:%M:%S") + " IST"

def to_ist_time_only(dt:Optional[datetime]) -> str:
    if dt is None:
        return "-"
    if dt.tzinfo is None:
        dt=dt.replace(tzinfo=timezone.utc)
    ist_zone=timezone(timedelta(hours=5, minutes=30))
    return dt.astimezone(ist_zone).strftime("%H:%M:%S") + " IST"

def format_duration(seconds:float) -> str:
    if seconds==0:
        return "0 ms"
    if seconds<1.0:
        return f"{seconds * 1000:.1f} ms"
    return f"{seconds:.2f} s"

async def get_metrics(db) -> Dict[str, Any]:
    status_counts={status.value:0 for status in TaskStatus}
    result=await db.execute(
        select(cast(Task.status, String), func.count(Task.id)).group_by(Task.status)
    )
    for status,count in result.all():
        if status:
            status_counts[status.lower()]=count

    high_len=await redis.llen(settings.high_queue_name)
    default_len=await redis.llen(settings.default_queue_name)
    low_len=await redis.llen(settings.low_queue_name)
    dlq_len=await redis.llen(settings.dead_letter_queue_name)
    scheduled_len=await redis.zcard(settings.scheduled_queue_name)

    time_query=text("""
        SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (e2.timestamp - e1.timestamp))), 0)
        FROM task_events e1
        JOIN task_events e2 ON e1.task_id = e2.task_id
        WHERE e1.event_type = :started AND e2.event_type = :completed
    """)
    time_result=await db.execute(time_query, {"started":TaskEventType.TASK_STARTED.name, "completed":TaskEventType.TASK_COMPLETED.name})
    avg_exec_time=time_result.scalar() or 0.0

    wait_query=text("""
        SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (e2.timestamp - e1.timestamp))), 0)
        FROM task_events e1
        JOIN task_events e2 ON e1.task_id = e2.task_id
        WHERE e1.event_type = :created AND e2.event_type = :started
    """)
    wait_result=await db.execute(wait_query, {"created":TaskEventType.TASK_CREATED.name, "started":TaskEventType.TASK_STARTED.name})
    avg_wait_time=wait_result.scalar() or 0.0

    return {
        "status_counts":status_counts,
        "queue_lengths":{
            "high":high_len,
            "default":default_len,
            "low":low_len,
            "dlq":dlq_len,
            "scheduled":scheduled_len
        },
        "avg_execution_time":format_duration(avg_exec_time),
        "avg_queue_wait_time":format_duration(avg_wait_time)
    }

async def get_active_workers(db) -> List[Dict[str, Any]]:
    pattern="orion:worker:*:heartbeat"
    cursor=0
    worker_names=[]
    while True:
        cursor,keys=await redis.scan(cursor, match=pattern, count=100)
        for key in keys:
            parts=key.split(":")
            if len(parts)>=3:
                worker_names.append(parts[2])
        if cursor==0:
            break

    workers=[]
    for name in worker_names:
        result=await db.execute(
            select(Task)
            .where(and_(Task.worker_id==name, Task.status==TaskStatus.RUNNING))
        )
        running_tasks=result.scalars().all()
        current_task=None
        lease_expiry=None
        status_str="Idle"
        if running_tasks:
            task=running_tasks[0]
            current_task=str(task.id)
            event_res=await db.execute(
                select(TaskEvent.timestamp)
                .where(and_(TaskEvent.task_id==task.id, TaskEvent.event_type==TaskEventType.TASK_STARTED))
                .order_by(TaskEvent.timestamp.desc())
                .limit(1)
            )
            started_ts=event_res.scalar()
            dur_ms=0
            if started_ts:
                now_utc=datetime.now(timezone.utc)
                if started_ts.tzinfo is None:
                    started_ts=started_ts.replace(tzinfo=timezone.utc)
                dur_ms=int((now_utc - started_ts).total_seconds() * 1000)

            status_str=f"Running | {task.task_type} | {dur_ms} ms"
            if task.retry_count > 0:
                status_str+=f" [Retry {task.retry_count}/{task.max_retries}]"

            if task.lease_expires_at:
                lease_expiry=to_ist_time_only(task.lease_expires_at)

        workers.append({
            "worker_id":name,
            "status":status_str,
            "current_task":current_task,
            "lease_expiry":lease_expiry
        })
    return workers

def decode_item(item) -> str:
    if isinstance(item, bytes):
        return item.decode("utf-8")
    return item

async def get_queue_contents() -> Dict[str, List[Dict[str, Any]]]:
    high_items=[decode_item(item) for item in await redis.lrange(settings.high_queue_name, 0, -1)]
    default_items=[decode_item(item) for item in await redis.lrange(settings.default_queue_name, 0, -1)]
    low_items=[decode_item(item) for item in await redis.lrange(settings.low_queue_name, 0, -1)]
    dlq_items=[decode_item(item) for item in await redis.lrange(settings.dead_letter_queue_name, 0, -1)]
    
    scheduled_raw=await redis.zrange(settings.scheduled_queue_name, 0, -1, withscores=True)
    scheduled_items=[]
    now_ts=datetime.now(timezone.utc).timestamp()
    for item,score in scheduled_raw:
        task_id=decode_item(item)
        rem_sec=int(score - now_ts)
        scheduled_items.append({
            "id":task_id,
            "remaining":f"Runs in {rem_sec}s" if rem_sec > 0 else "Promoting..."
        })

    return {
        "high":[{"id":tid} for tid in high_items],
        "default":[{"id":tid} for tid in default_items],
        "low":[{"id":tid} for tid in low_items],
        "dlq":[{"id":tid} for tid in dlq_items],
        "scheduled":scheduled_items
    }

async def get_event_feed(db) -> List[Dict[str, Any]]:
    result=await db.execute(
        select(TaskEvent)
        .options(selectinload(TaskEvent.task))
        .order_by(TaskEvent.timestamp.desc())
        .limit(100)
    )
    events=result.scalars().all()
    feed=[]
    for event in events:
        feed.append({
            "timestamp":to_ist_time_only(event.timestamp),
            "task_id":str(event.task_id),
            "event_type":event.event_type.value,
            "worker_id":event.worker_id,
            "details":event.details
        })
    return feed

async def get_heatmap(db) -> List[Dict[str, Any]]:
    comp_val=TaskEventType.TASK_COMPLETED.value.upper()
    fail_val=TaskEventType.TASK_FAILED.value.upper()
    heatmap_query=text(f"""
        SELECT EXTRACT(HOUR FROM timestamp) as hr, COUNT(*) as cnt
        FROM task_events
        WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
          AND event_type IN ('{comp_val}', '{fail_val}')
        GROUP BY hr
        ORDER BY hr ASC
    """)
    result=await db.execute(heatmap_query)
    rows=result.all()
    hour_counts={int(row[0]): int(row[1]) for row in rows}
    
    current_hour=datetime.now(timezone.utc).hour
    data=[]
    for i in range(24):
        hour=(current_hour-23+i)%24
        count=hour_counts.get(hour, 0)
        data.append({
            "hour":f"{hour:02d}:00",
            "count":count
        })
    return data

async def get_recent_tasks(db) -> List[Dict[str, Any]]:
    result=await db.execute(
        select(Task)
        .order_by(Task.created_at.desc())
        .limit(20)
    )
    tasks=result.scalars().all()
    recent=[]
    for task in tasks:
        recent.append({
            "id":str(task.id),
            "task_type":task.task_type,
            "status":task.status.value,
            "priority":task.priority.value,
            "created_at":to_ist_time_only(task.created_at)
        })
    return recent
