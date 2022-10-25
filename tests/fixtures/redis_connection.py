from redis.asyncio.connection import ConnectionPool as AsyncConnectionPool
from redis.connection import ConnectionPool as SyncConnectionPool
import pytest


@pytest.fixture
async def return_connection_pool_for_async_redis():
    conn = AsyncConnectionPool(host="localhost", port=6379, db=0)
    yield conn

@pytest.fixture
def return_connection_pool_for_sync_redis():
    conn = SyncConnectionPool(host="localhost", port=6379, db=0)
    yield conn

