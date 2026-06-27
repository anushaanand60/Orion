from fastapi import FastAPI
from orion.routes.tasks import router as tasks_router

app=FastAPI(title="Orion")

app.include_router(tasks_router)

@app.get("/")
async def root():
    return {"service":"orion"}
