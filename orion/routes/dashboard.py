import uuid
import os
import asyncio
import time
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from orion.database import get_db
from orion.config import settings
from orion.models.task import Task
from orion.models.task_event import TaskEvent
from orion.enums import TaskStatus, TaskEventType
from orion.events import record_event
from orion.redis import redis
from orion.services import dashboard_service

router=APIRouter()

templates_dir=os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
templates=Jinja2Templates(directory=templates_dir)

@router.get("", response_class=HTMLResponse, tags=["Dashboard"])
async def get_dashboard(request:Request, db:AsyncSession=Depends(get_db)):
    metrics=await dashboard_service.get_metrics(db)
    workers=await dashboard_service.get_active_workers(db)
    queues=await dashboard_service.get_queue_contents()
    events=await dashboard_service.get_event_feed(db)
    heatmap=await dashboard_service.get_heatmap(db)
    recent_tasks=await dashboard_service.get_recent_tasks(db)
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request":request,
            "metrics":metrics,
            "workers":workers,
            "queues":queues,
            "events":events,
            "heatmap":heatmap,
            "recent_tasks":recent_tasks
        }
    )

@router.get("/metrics", response_class=HTMLResponse, tags=["Dashboard"])
async def get_metrics_partial(request:Request, db:AsyncSession=Depends(get_db)):
    metrics=await dashboard_service.get_metrics(db)
    workers=await dashboard_service.get_active_workers(db)
    queues=await dashboard_service.get_queue_contents()
    events=await dashboard_service.get_event_feed(db)
    heatmap=await dashboard_service.get_heatmap(db)
    recent_tasks=await dashboard_service.get_recent_tasks(db)
    return templates.TemplateResponse(
        "metrics_partial.html",
        {
            "request":request,
            "metrics":metrics,
            "workers":workers,
            "queues":queues,
            "events":events,
            "heatmap":heatmap,
            "recent_tasks":recent_tasks
        }
    )

@router.get("/tasks/{task_id}", response_class=HTMLResponse, tags=["Dashboard"])
async def get_task_details(task_id:uuid.UUID, request:Request, db:AsyncSession=Depends(get_db)):
    result=await db.execute(
        select(Task)
        .options(selectinload(Task.parents), selectinload(Task.children), selectinload(Task.events))
        .where(Task.id==task_id)
    )
    task=result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    svg_width=600
    svg_height=300
    nodes=[]
    edges=[]

    parent_count=len(task.parents)
    if parent_count>0:
        for i,parent in enumerate(task.parents):
            y=int(svg_height/(parent_count+1)*(i+1))
            nodes.append({"id":str(parent.id), "label":f"{parent.task_type} (parent)", "status":parent.status.value, "x":100, "y":y})
            edges.append({"from_x":100, "from_y":y, "to_x":300, "to_y":150})

    nodes.append({"id":str(task.id), "label":f"{task.task_type} (target)", "status":task.status.value, "x":300, "y":150})

    child_count=len(task.children)
    if child_count>0:
        for i,child in enumerate(task.children):
            y=int(svg_height/(child_count+1)*(i+1))
            nodes.append({"id":str(child.id), "label":f"{child.task_type} (child)", "status":child.status.value, "x":500, "y":y})
            edges.append({"from_x":300, "from_y":150, "to_x":500, "to_y":y})

    blocked_by_failed=False
    blocked_by_pending=[]
    if task.status==TaskStatus.BLOCKED:
        for parent in task.parents:
            if parent.status==TaskStatus.FAILED:
                blocked_by_failed=True
            elif parent.status!=TaskStatus.COMPLETED:
                blocked_by_pending.append({
                    "id":str(parent.id),
                    "task_type":parent.task_type,
                    "status":parent.status.value
                })

    formatted_events=[]
    for ev in task.events:
        formatted_events.append({
            "timestamp":dashboard_service.to_ist_str(ev.timestamp),
            "event_type":ev.event_type.value,
            "worker_id":ev.worker_id,
            "details":ev.details
        })

    return templates.TemplateResponse(
        "task_detail.html",
        {
            "request":request,
            "task":task,
            "nodes":nodes,
            "edges":edges,
            "svg_width":svg_width,
            "svg_height":svg_height,
            "events":formatted_events,
            "blocked_by_failed":blocked_by_failed,
            "blocked_by_pending":blocked_by_pending
        }
    )

@router.post("/benchmark", tags=["Benchmark"])
async def run_benchmark(db:AsyncSession=Depends(get_db)):
    start_time=time.time()
    task_ids=[]
    for i in range(100):
        task=Task(
            id=uuid.uuid4(),
            status=TaskStatus.PENDING,
            task_type="echo",
            payload={"benchmark":i},
            result=None
        )
        db.add(task)
        await record_event(db, task.id, TaskEventType.TASK_CREATED)
        await record_event(db, task.id, TaskEventType.TASK_ENQUEUED)
        task_ids.append(task.id)
    await db.commit()

    for tid in task_ids:
        await redis.rpush(settings.default_queue_name, str(tid))

    all_done=False
    for _ in range(60):
        await asyncio.sleep(0.1)
        async with db.begin_nested() as nested:
            result=await db.execute(
                select(func.count(Task.id))
                .where(and_(Task.id.in_(task_ids), Task.status.in_([TaskStatus.COMPLETED, TaskStatus.FAILED])))
            )
            count=result.scalar() or 0
            if count==100:
                all_done=True
                break
    
    end_time=time.time()
    elapsed=end_time-start_time
    throughput=100/elapsed if elapsed>0 else 0

    if all_done:
        # Fetch events to compute real execution latencies
        query=select(TaskEvent).where(
            and_(
                TaskEvent.task_id.in_(task_ids),
                TaskEvent.event_type.in_([TaskEventType.TASK_CREATED, TaskEventType.TASK_COMPLETED, TaskEventType.TASK_FAILED])
            )
        )
        res_events=await db.execute(query)
        events=res_events.scalars().all()
        
        task_times={}
        for event in events:
            tid=str(event.task_id)
            if tid not in task_times:
                task_times[tid]={}
            if event.event_type==TaskEventType.TASK_CREATED:
                task_times[tid]["created"]=event.timestamp
            else:
                task_times[tid]["completed"]=event.timestamp
        
        latencies=[]
        for tid, times in task_times.items():
            if "created" in times and "completed" in times:
                duration=(times["completed"] - times["created"]).total_seconds() * 1000
                latencies.append(duration)
        
        latencies.sort()
        count=len(latencies)
        avg_lat=sum(latencies)/count if count>0 else 0
        median=latencies[count//2] if count>0 else 0
        p95=latencies[int(count*0.95)] if count>0 else 0
        p99=latencies[int(count*0.99)] if count>0 else 0
        fastest=latencies[0] if count>0 else 0
        slowest=latencies[-1] if count>0 else 0
    else:
        avg_lat=median=p95=p99=fastest=slowest=0

    return {
        "success":all_done,
        "tasks_submitted":100,
        "elapsed_seconds":round(elapsed, 2),
        "throughput_tps":round(throughput, 2),
        "avg_latency_ms":round(avg_lat, 1),
        "median_latency_ms":round(median, 1),
        "p95_latency_ms":round(p95, 1),
        "p99_latency_ms":round(p99, 1),
        "fastest_ms":round(fastest, 1),
        "slowest_ms":round(slowest, 1)
    }
