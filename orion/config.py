import socket
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    host:str="127.0.0.1"
    port:int=8000
    database_url:str="postgresql+asyncpg://postgres:postgres@localhost:5432/orion"
    redis_url:str="redis://localhost:6379/0"
    queue_name:str="orion:queue:default"
    worker_name:str=socket.gethostname()
    model_config=SettingsConfigDict(env_file=".env")

settings=Settings()
