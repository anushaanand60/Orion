from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from orion.config import settings

engine=create_async_engine(settings.database_url)
async_session=async_sessionmaker(bind=engine, expire_on_commit=False)
