import socket
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    host:str="127.0.0.1"
    port:int=8000
    database_url:str="postgresql+asyncpg://postgres:postgres@localhost:5432/orion"
    redis_url:str="redis://localhost:6379/0"
    queue_name:str="orion:queue:default"
    high_queue_name:str="orion:queue:high"
    default_queue_name:str="orion:queue:default"
    low_queue_name:str="orion:queue:low"
    dead_letter_queue_name:str="orion:queue:dead_letter"
    worker_name:str=socket.gethostname()
    worker_lease_seconds:int=30
    lease_recovery_interval:int=10
    model_config=SettingsConfigDict(env_file=".env")

settings=Settings()
