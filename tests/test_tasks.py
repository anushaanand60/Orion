import asyncio
import uuid
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from orion.main import app
from orion.database import async_session, engine
from orion.models.task import Task
from orion.redis import redis
from redis.asyncio import ConnectionPool
from orion.config import settings

@pytest.fixture
def anyio_backend():
    return "asyncio"

@pytest.fixture(autouse=True)
async def cleanup_connections():
    yield
    await redis.delete(
        settings.scheduled_queue_name,
        settings.high_queue_name,
        settings.default_queue_name,
        settings.low_queue_name,
        settings.dead_letter_queue_name
    )
    await engine.dispose()
    await redis.aclose()
    redis.connection_pool=ConnectionPool.from_url(settings.redis_url, decode_responses=True)

@pytest.mark.anyio
async def test_create_task():
    payload={"foo":"bar"}
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response=await ac.post("/tasks", json={"task_type":"echo","payload":payload})
    assert response.status_code==201
    data=response.json()
    assert "id" in data
    assert data["status"]=="pending"
    task_id=data["id"]

    async with async_session() as session:
        result=await session.execute(select(Task).where(Task.id==uuid.UUID(task_id)))
        db_task=result.scalar_one_or_none()

    assert db_task is not None
    assert str(db_task.id)==task_id
    assert db_task.status=="pending"
    assert db_task.task_type=="echo"
    assert db_task.retry_count==0
    assert db_task.max_retries==3
    assert db_task.payload==payload
    assert db_task.result is None

@pytest.mark.anyio
async def test_get_task_success():
    payload={"hello":"world"}
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        create_res=await ac.post("/tasks", json={"task_type":"echo","payload":payload})
        assert create_res.status_code==201
        task_id=create_res.json()["id"]

        get_res=await ac.get(f"/tasks/{task_id}")
        assert get_res.status_code==200
        data=get_res.json()
        assert data["id"]==task_id
        assert data["status"]=="pending"
        assert data["task_type"]=="echo"
        assert data["retry_count"]==0
        assert data["max_retries"]==3
        assert data["payload"]==payload
        assert data["result"] is None
        assert "created_at" in data
        assert "updated_at" in data

@pytest.mark.anyio
async def test_get_task_not_found():
    random_id=uuid.uuid4()
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response=await ac.get(f"/tasks/{random_id}")
    assert response.status_code==404
    assert response.json()=={"detail":"Task not found"}

@pytest.mark.anyio
async def test_background_task_execution():
    payload={"test":"execution"}
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        create_res=await ac.post("/tasks", json={"task_type":"echo","payload":payload})
        assert create_res.status_code==201
        task_id=create_res.json()["id"]

        completed=False
        for _ in range(20):
            get_res=await ac.get(f"/tasks/{task_id}")
            assert get_res.status_code==200
            data=get_res.json()
            if data["status"]=="completed":
                completed=True
                assert data["task_type"]=="echo"
                assert data["result"]==payload
                break
            await asyncio.sleep(0.1)
        assert completed is True

@pytest.mark.anyio
async def test_background_task_sum_success():
    payload={"numbers":[10,20,30]}
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        create_res=await ac.post("/tasks", json={"task_type":"sum","payload":payload})
        assert create_res.status_code==201
        task_id=create_res.json()["id"]

        completed=False
        for _ in range(20):
            get_res=await ac.get(f"/tasks/{task_id}")
            assert get_res.status_code==200
            data=get_res.json()
            if data["status"]=="completed":
                completed=True
                assert data["result"]=={"sum":60}
                break
            await asyncio.sleep(0.1)
        assert completed is True

@pytest.mark.anyio
async def test_background_task_sum_invalid_payload():
    payload={"numbers":"not-a-list"}
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        create_res=await ac.post("/tasks", json={"task_type":"sum","payload":payload})
        assert create_res.status_code==201
        task_id=create_res.json()["id"]

        finished=False
        for _ in range(20):
            get_res=await ac.get(f"/tasks/{task_id}")
            assert get_res.status_code==200
            data=get_res.json()
            if data["status"]=="failed":
                finished=True
                assert "error" in data["result"]
                assert data["retry_count"]==0
                break
            await asyncio.sleep(0.1)
        assert finished is True

@pytest.mark.anyio
async def test_background_task_unknown_type_failure():
    payload={"foo":"bar"}
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        create_res=await ac.post("/tasks", json={"task_type":"unknown_type","payload":payload})
        assert create_res.status_code==201
        task_id=create_res.json()["id"]

        finished=False
        for _ in range(20):
            get_res=await ac.get(f"/tasks/{task_id}")
            assert get_res.status_code==200
            data=get_res.json()
            if data["status"]=="failed":
                finished=True
                assert data["result"]=={"error":"Unknown task type: unknown_type"}
                assert data["retry_count"]==0
                break
            await asyncio.sleep(0.1)
        assert finished is True

@pytest.mark.anyio
async def test_background_task_retry_exhausted_failure():
    payload={}
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        create_res=await ac.post("/tasks", json={"task_type":"fail","max_retries":2,"payload":payload})
        assert create_res.status_code==201
        task_id=create_res.json()["id"]

        finished=False
        for _ in range(20):
            get_res=await ac.get(f"/tasks/{task_id}")
            assert get_res.status_code==200
            data=get_res.json()
            if data["status"]=="failed":
                finished=True
                assert data["retry_count"]==3
                assert data["max_retries"]==2
                assert data["result"]=={"error":"Simulated failure"}
                break
            await asyncio.sleep(0.1)
        assert finished is True

@pytest.mark.anyio
async def test_concurrent_worker_execution():
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        task_ids=[]
        for i in range(5):
            res=await ac.post("/tasks", json={"task_type":"echo","payload":{"idx":i}})
            assert res.status_code==201
            task_ids.append(res.json()["id"])

        finished=False
        for _ in range(50):
            terminal_count=0
            for task_id in task_ids:
                get_res=await ac.get(f"/tasks/{task_id}")
                assert get_res.status_code==200
                data=get_res.json()
                if data["status"] in ["completed","failed"]:
                    terminal_count+=1
            if terminal_count==len(task_ids):
                finished=True
                break
            await asyncio.sleep(0.1)
        assert finished is True

        high_len=await redis.llen(settings.high_queue_name)
        default_len=await redis.llen(settings.default_queue_name)
        low_len=await redis.llen(settings.low_queue_name)
        assert high_len+default_len+low_len==0

        for task_id in task_ids:
            get_res=await ac.get(f"/tasks/{task_id}")
            data=get_res.json()
            assert data["status"]=="completed"
            assert data["retry_count"]==0

@pytest.mark.anyio
async def test_priority_queue_scheduling():
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        low_ids=[]
        for i in range(3):
            res=await ac.post("/tasks", json={
                "task_type":"slow_echo",
                "payload":{"idx":i,"delay":5},
                "priority":"low"
            })
            assert res.status_code==201
            low_ids.append(res.json()["id"])

        res=await ac.post("/tasks", json={
            "task_type":"echo",
            "payload":{"priority_test":"high"},
            "priority":"high"
        })
        assert res.status_code==201
        high_id=res.json()["id"]

        high_completed_while_lows_pending=False
        for _ in range(60):
            high_res=await ac.get(f"/tasks/{high_id}")
            high_data=high_res.json()
            if high_data["status"]=="completed":
                low_statuses=[]
                for task_id in low_ids:
                    low_res=await ac.get(f"/tasks/{task_id}")
                    low_statuses.append(low_res.json()["status"])
                if any(s in ["pending","running"] for s in low_statuses):
                    high_completed_while_lows_pending=True
                break
            await asyncio.sleep(0.2)

        assert high_data["status"]=="completed"
        assert high_completed_while_lows_pending is True

        all_tasks_done=False
        for _ in range(60):
            terminal_count=0
            for task_id in low_ids:
                get_res=await ac.get(f"/tasks/{task_id}")
                if get_res.json()["status"] in ["completed","failed"]:
                    terminal_count+=1
            if terminal_count==len(low_ids):
                all_tasks_done=True
                break
            await asyncio.sleep(0.5)
        assert all_tasks_done is True

@pytest.mark.anyio
async def test_expired_lease_recovery():
    from datetime import datetime, timedelta, timezone
    from orion.recovery import recover_expired_leases

    task_id=uuid.uuid4()
    async with async_session() as db:
        task=Task(
            id=task_id,
            status="running",
            task_type="echo",
            payload={"test":"recovery"},
            priority="default",
            worker_id="dead-worker",
            lease_expires_at=datetime.now(timezone.utc)-timedelta(seconds=60),
        )
        db.add(task)
        await db.commit()

    await recover_expired_leases()

    async with async_session() as db:
        result=await db.execute(select(Task).where(Task.id==task_id))
        task=result.scalar_one()
        assert task.status=="pending"
        assert task.worker_id is None
        assert task.lease_expires_at is None

    completed=False
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        for _ in range(30):
            get_res=await ac.get(f"/tasks/{task_id}")
            if get_res.json()["status"]=="completed":
                completed=True
                break
            await asyncio.sleep(0.2)
    assert completed is True

@pytest.mark.anyio
async def test_completed_tasks_not_recovered():
    from datetime import datetime, timezone
    from orion.recovery import recover_expired_leases

    task_id=uuid.uuid4()
    async with async_session() as db:
        task=Task(
            id=task_id,
            status="completed",
            task_type="echo",
            payload={"test":"completed"},
            result={"test":"completed"},
            worker_id=None,
            lease_expires_at=None,
        )
        db.add(task)
        await db.commit()

    await recover_expired_leases()

    async with async_session() as db:
        result=await db.execute(select(Task).where(Task.id==task_id))
        task=result.scalar_one()
        assert task.status=="completed"

@pytest.mark.anyio
async def test_active_lease_not_recovered():
    from datetime import datetime, timedelta, timezone
    from orion.recovery import recover_expired_leases

    task_id=uuid.uuid4()
    future_lease=datetime.now(timezone.utc)+timedelta(minutes=5)
    async with async_session() as db:
        task=Task(
            id=task_id,
            status="running",
            task_type="echo",
            payload={"test":"active"},
            worker_id="live-worker",
            lease_expires_at=future_lease,
        )
        db.add(task)
        await db.commit()

    await recover_expired_leases()

    async with async_session() as db:
        result=await db.execute(select(Task).where(Task.id==task_id))
        task=result.scalar_one()
        assert task.status=="running"
        assert task.worker_id=="live-worker"

@pytest.mark.anyio
async def test_permanent_failure_sent_to_dlq():
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        res=await ac.post("/tasks", json={"task_type":"sum","payload":{"numbers":"bad"}})
        assert res.status_code==201
        task_id=res.json()["id"]

        finished=False
        for _ in range(20):
            get_res=await ac.get(f"/tasks/{task_id}")
            if get_res.json()["status"]=="failed":
                finished=True
                break
            await asyncio.sleep(0.1)
        assert finished is True

        dlq_list=await redis.lrange(settings.dead_letter_queue_name, 0, -1)
        assert task_id in dlq_list

        dlq_res=await ac.get("/dead-letter")
        assert dlq_res.status_code==200
        dlq_tasks=dlq_res.json()
        assert any(t["id"]==task_id for t in dlq_tasks)

@pytest.mark.anyio
async def test_retry_exhaustion_sent_to_dlq():
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        res=await ac.post("/tasks", json={"task_type":"fail","max_retries":2,"payload":{}})
        assert res.status_code==201
        task_id=res.json()["id"]

        finished=False
        for _ in range(30):
            get_res=await ac.get(f"/tasks/{task_id}")
            if get_res.json()["status"]=="failed":
                finished=True
                break
            await asyncio.sleep(0.1)
        assert finished is True

        dlq_list=await redis.lrange(settings.dead_letter_queue_name, 0, -1)
        assert task_id in dlq_list

@pytest.mark.anyio
async def test_successful_task_not_in_dlq():
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        res=await ac.post("/tasks", json={"task_type":"echo","payload":{"foo":"bar"}})
        assert res.status_code==201
        task_id=res.json()["id"]

        finished=False
        for _ in range(20):
            get_res=await ac.get(f"/tasks/{task_id}")
            if get_res.json()["status"]=="completed":
                finished=True
                break
            await asyncio.sleep(0.1)
        assert finished is True

        dlq_list=await redis.lrange(settings.dead_letter_queue_name, 0, -1)
        assert task_id not in dlq_list

@pytest.mark.anyio
async def test_recovered_task_not_in_dlq():
    from datetime import datetime, timedelta, timezone
    from orion.recovery import recover_expired_leases

    task_id=uuid.uuid4()
    async with async_session() as db:
        task=Task(
            id=task_id,
            status="running",
            task_type="echo",
            payload={"test":"recovered-dlq"},
            priority="default",
            worker_id="dead-worker",
            lease_expires_at=datetime.now(timezone.utc)-timedelta(seconds=60),
        )
        db.add(task)
        await db.commit()

    await recover_expired_leases()

    completed=False
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        for _ in range(30):
            get_res=await ac.get(f"/tasks/{task_id}")
            if get_res.json()["status"]=="completed":
                completed=True
                break
            await asyncio.sleep(0.2)
    assert completed is True

    dlq_list=await redis.lrange(settings.dead_letter_queue_name, 0, -1)
    assert str(task_id) not in dlq_list

@pytest.mark.anyio
async def test_dlq_contains_only_terminal_failures():
    from datetime import datetime, timedelta, timezone
    from orion.recovery import recover_expired_leases

    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        res_success=await ac.post("/tasks", json={"task_type":"echo","payload":{}})
        assert res_success.status_code==201
        success_id=res_success.json()["id"]

        res_retryable=await ac.post("/tasks", json={"task_type":"fail","max_retries":5,"payload":{"delay":2}})
        assert res_retryable.status_code==201
        retryable_id=res_retryable.json()["id"]

        res_permanent=await ac.post("/tasks", json={"task_type":"sum","payload":{"numbers":"bad"}})
        assert res_permanent.status_code==201
        permanent_id=res_permanent.json()["id"]

        res_exhausted=await ac.post("/tasks", json={"task_type":"fail","max_retries":1,"payload":{}})
        assert res_exhausted.status_code==201
        exhausted_id=res_exhausted.json()["id"]

        recovered_id=uuid.uuid4()
        async with async_session() as db:
            task=Task(
                id=recovered_id,
                status="running",
                task_type="echo",
                payload={"test":"policy"},
                priority="default",
                worker_id="dead-worker",
                lease_expires_at=datetime.now(timezone.utc)-timedelta(seconds=60),
            )
            db.add(task)
            await db.commit()

        await recover_expired_leases()

        done=False
        for _ in range(40):
            res_s=await ac.get(f"/tasks/{success_id}")
            res_p=await ac.get(f"/tasks/{permanent_id}")
            res_e=await ac.get(f"/tasks/{exhausted_id}")
            res_r=await ac.get(f"/tasks/{recovered_id}")
            res_ret=await ac.get(f"/tasks/{retryable_id}")

            s_status=res_s.json()["status"]
            p_status=res_p.json()["status"]
            e_status=res_e.json()["status"]
            r_status=res_r.json()["status"]
            ret_data=res_ret.json()

            if (s_status=="completed" and 
                p_status=="failed" and 
                e_status=="failed" and 
                r_status=="completed" and 
                ret_data["retry_count"]>=1):
                done=True
                break
            await asyncio.sleep(0.2)
        assert done is True

        dlq_list=await redis.lrange(settings.dead_letter_queue_name, 0, -1)
        
        assert permanent_id in dlq_list
        assert exhausted_id in dlq_list
        
        assert success_id not in dlq_list
        assert retryable_id not in dlq_list
        assert str(recovered_id) not in dlq_list

@pytest.mark.anyio
async def test_immediate_task_executes_normally():
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        res=await ac.post("/tasks", json={"task_type":"echo","payload":{"immediate":"test"}})
        assert res.status_code==201
        task_id=res.json()["id"]

        completed=False
        for _ in range(20):
            get_res=await ac.get(f"/tasks/{task_id}")
            if get_res.json()["status"]=="completed":
                completed=True
                break
            await asyncio.sleep(0.1)
        assert completed is True

@pytest.mark.anyio
async def test_future_task_not_executed_early():
    from datetime import datetime, timezone, timedelta
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        future_time=datetime.now(timezone.utc)+timedelta(seconds=5)
        res=await ac.post("/tasks", json={
            "task_type":"echo",
            "payload":{"future":"test"},
            "scheduled_at":future_time.isoformat()
        })
        assert res.status_code==201
        task_id=res.json()["id"]

        for _ in range(10):
            get_res=await ac.get(f"/tasks/{task_id}")
            assert get_res.json()["status"]=="pending"
            await asyncio.sleep(0.1)

        scheduled_tasks=await redis.zrange(settings.scheduled_queue_name, 0, -1)
        assert task_id in scheduled_tasks

        high_len=await redis.llen(settings.high_queue_name)
        default_len=await redis.llen(settings.default_queue_name)
        low_len=await redis.llen(settings.low_queue_name)
        assert high_len+default_len+low_len==0

@pytest.mark.anyio
async def test_scheduler_promotes_ready_task():
    from datetime import datetime, timezone, timedelta
    from orion.scheduler import promote_ready_tasks
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        future_time=datetime.now(timezone.utc)+timedelta(seconds=1)
        res=await ac.post("/tasks", json={
            "task_type":"echo",
            "payload":{"ready":"test"},
            "scheduled_at":future_time.isoformat()
        })
        assert res.status_code==201
        task_id=res.json()["id"]

        await asyncio.sleep(1.2)
        await promote_ready_tasks()

        completed=False
        for _ in range(20):
            get_res=await ac.get(f"/tasks/{task_id}")
            if get_res.json()["status"]=="completed":
                completed=True
                break
            await asyncio.sleep(0.1)
        assert completed is True

@pytest.mark.anyio
async def test_priority_preserved_after_scheduling():
    from datetime import datetime, timezone, timedelta
    from orion.scheduler import promote_ready_tasks
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        future_time=datetime.now(timezone.utc)+timedelta(seconds=1)
        res_low=await ac.post("/tasks", json={
            "task_type":"slow_echo",
            "payload":{"low":"priority","delay":3},
            "priority":"low",
            "scheduled_at":future_time.isoformat()
        })
        assert res_low.status_code==201
        low_id=res_low.json()["id"]

        res_high=await ac.post("/tasks", json={
            "task_type":"echo",
            "payload":{"high":"priority"},
            "priority":"high",
            "scheduled_at":future_time.isoformat()
        })
        assert res_high.status_code==201
        high_id=res_high.json()["id"]

        await asyncio.sleep(1.2)
        await promote_ready_tasks()

        high_completed_while_low_pending=False
        for _ in range(30):
            high_res=await ac.get(f"/tasks/{high_id}")
            high_data=high_res.json()
            if high_data["status"]=="completed":
                low_res=await ac.get(f"/tasks/{low_id}")
                if low_res.json()["status"] in ["pending","running"]:
                    high_completed_while_low_pending=True
                break
            await asyncio.sleep(0.1)
        assert high_completed_while_low_pending is True

        completed_low=False
        for _ in range(40):
            low_res=await ac.get(f"/tasks/{low_id}")
            if low_res.json()["status"]=="completed":
                completed_low=True
                break
            await asyncio.sleep(0.1)
        assert completed_low is True

@pytest.mark.anyio
async def test_scheduler_only_promotes_ready_tasks():
    from datetime import datetime, timezone, timedelta
    from orion.scheduler import promote_ready_tasks
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        future_time_ready=datetime.now(timezone.utc)+timedelta(seconds=1)
        res_ready=await ac.post("/tasks", json={
            "task_type":"echo",
            "payload":{"ready":"only"},
            "scheduled_at":future_time_ready.isoformat()
        })
        assert res_ready.status_code==201
        ready_id=res_ready.json()["id"]

        future_time_not_ready=datetime.now(timezone.utc)+timedelta(seconds=10)
        res_not_ready=await ac.post("/tasks", json={
            "task_type":"echo",
            "payload":{"not":"ready"},
            "scheduled_at":future_time_not_ready.isoformat()
        })
        assert res_not_ready.status_code==201
        not_ready_id=res_not_ready.json()["id"]

        await asyncio.sleep(1.2)
        await promote_ready_tasks()

        completed=False
        for _ in range(20):
            get_res=await ac.get(f"/tasks/{ready_id}")
            if get_res.json()["status"]=="completed":
                completed=True
                break
            await asyncio.sleep(0.1)
        assert completed is True

        get_res_not_ready=await ac.get(f"/tasks/{not_ready_id}")
        assert get_res_not_ready.json()["status"]=="pending"

        scheduled_tasks=await redis.zrange(settings.scheduled_queue_name, 0, -1)
        assert not_ready_id in scheduled_tasks
        assert ready_id not in scheduled_tasks
