from redis.asyncio.connection import ConnectionPool as AsyncConnectionPool
from redis.connection import ConnectionPool as SyncConnectionPool
import pytest, os

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

@pytest.fixture
async def return_connection_pool_for_async_redis():
    conn = AsyncConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=0)
    yield conn

@pytest.fixture
def return_connection_pool_for_sync_redis():
    conn = SyncConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=0)
    yield conn

