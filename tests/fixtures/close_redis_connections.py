
import pytest
from redis.asyncio import Redis as AsyncRedis
from redis import Redis as SyncRedis

@pytest.fixture(autouse=True)
async def close_redis_connections(return_connection_pool_for_async_redis, return_connection_pool_for_sync_redis):
    await AsyncRedis(connection_pool=return_connection_pool_for_async_redis).flushall()
    SyncRedis(connection_pool=return_connection_pool_for_sync_redis).flushall()
    yield
    await return_connection_pool_for_async_redis.disconnect()
    return_connection_pool_for_sync_redis.disconnect()