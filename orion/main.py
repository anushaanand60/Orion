from fastapi import FastAPI
from orion.routes.tasks import router as tasks_router
from orion.routes.dashboard import router as dashboard_router

app=FastAPI(title="Orion")

app.include_router(tasks_router)
app.include_router(dashboard_router, prefix="/dashboard")

@app.get("/")
async def root():
    return {"service":"orion"}
