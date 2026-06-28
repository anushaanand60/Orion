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
        response=await ac.post("/tasks", json={"payload":payload})
    assert response.status_code==201
    data=response.json()
    assert "id" in data
    assert data["status"]=="pending"
    task_id=data["id"]

    async with async_session() as session:
        result=await session.execute(select(Task).where(Task.id==task_id))
        db_task=result.scalar_one_or_none()

    assert db_task is not None
    assert str(db_task.id)==task_id
    assert db_task.status=="pending"
    assert db_task.payload==payload
    assert db_task.result is None

@pytest.mark.anyio
async def test_get_task_success():
    payload={"hello":"world"}
    transport=ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        create_res=await ac.post("/tasks", json={"payload":payload})
        assert create_res.status_code==201
        task_id=create_res.json()["id"]

        get_res=await ac.get(f"/tasks/{task_id}")
        assert get_res.status_code==200
        data=get_res.json()
        assert data["id"]==task_id
        assert data["status"]=="pending"
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
        create_res=await ac.post("/tasks", json={"payload":payload})
        assert create_res.status_code==201
        task_id=create_res.json()["id"]

        completed=False
        for _ in range(20):
            get_res=await ac.get(f"/tasks/{task_id}")
            assert get_res.status_code==200
            data=get_res.json()
            if data["status"]=="completed":
                completed=True
                assert data["result"]=={"message":"Task completed"}
                break
            await asyncio.sleep(0.1)
        assert completed is True
