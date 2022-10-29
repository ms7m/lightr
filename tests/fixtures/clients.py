

from ridant.main import RidantCache
from ridant.asyncio.main import RidantCache as AsyncRidantCache
import pytest

@pytest.fixture()
def ridant_client(return_connection_pool_for_sync_redis):
    return RidantCache(connection_pool=return_connection_pool_for_sync_redis)

@pytest.fixture()
async def async_ridant_client(return_connection_pool_for_async_redis):
    return AsyncRidantCache(connection_pool=return_connection_pool_for_async_redis)