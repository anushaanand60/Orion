from fastapi import FastAPI

app=FastAPI(title="Orion")

@app.get("/")
async def root():
    return {"service":"orion"}
