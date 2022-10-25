
import pytest

@pytest.fixture(autouse=True)
async def close_redis_connections(return_connection_pool_for_async_redis, return_connection_pool_for_sync_redis):
    yield
    await return_connection_pool_for_async_redis.disconnect()
    return_connection_pool_for_sync_redis.disconnect()