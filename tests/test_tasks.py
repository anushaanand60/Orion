import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from orion.main import app
from orion.database import async_session
from orion.models.task import Task

@pytest.fixture
def anyio_backend():
    return "asyncio"

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
