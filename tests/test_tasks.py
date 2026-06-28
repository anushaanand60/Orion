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
