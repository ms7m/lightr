

from lightr.asyncio.main import PydanticRedisCaching
from pydantic import BaseModel


class SamplePydanticModel(BaseModel):        
    name: str
    age: int
    


async def test_cache_item(return_connection_pool_for_async_redis):
    cache = PydanticRedisCaching(redis_connection_pool=return_connection_pool_for_async_redis)
    await cache.cache(SamplePydanticModel(name="test", age=1), "test")
    assert await cache.find_one(SamplePydanticModel, "test") == SamplePydanticModel(name="test", age=1)