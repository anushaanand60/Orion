from redis.asyncio import ConnectionPool, Redis
from orion.config import settings

pool=ConnectionPool.from_url(settings.redis_url, decode_responses=True)
redis=Redis(connection_pool=pool)
