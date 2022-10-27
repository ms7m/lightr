

from ridant.asyncio.main import RidantCache
from pydantic import BaseModel


class SamplePydanticModel(BaseModel):        
    name: str
    age: int
    


async def test_cache_item(return_connection_pool_for_async_redis):
    cache = RidantCache(redis_connection_pool=return_connection_pool_for_async_redis)
    await cache.cache(SamplePydanticModel(name="test", age=1), "test")
    assert await cache.find_one(SamplePydanticModel, "test") == SamplePydanticModel(name="test", age=1)
    
async def test_delete_cache_item(return_connection_pool_for_async_redis):
    cache = RidantCache(redis_connection_pool=return_connection_pool_for_async_redis)
    await cache.cache(SamplePydanticModel(name="test", age=1), "test")
    assert await cache.find_one(SamplePydanticModel, "test") == SamplePydanticModel(name="test", age=1)
    assert await cache.delete(SamplePydanticModel, "test") == True
    assert await cache.find_one(SamplePydanticModel, "test") == None
    
    
async def test_hash_set_cache_item(return_connection_pool_for_async_redis):
    cache = RidantCache(redis_connection_pool=return_connection_pool_for_async_redis)
    await cache.cache(SamplePydanticModel(name="test", age=1), "test", hash=True)
    await cache.find_one(SamplePydanticModel, "test", "name") == "test"
    await cache.find_one(SamplePydanticModel, "test", "age") == 1

async def test_hash_update_cache_item(return_connection_pool_for_async_redis):
    cache = RidantCache(redis_connection_pool=return_connection_pool_for_async_redis)
    await cache.cache(SamplePydanticModel(name="test", age=1), "test", hash=True)
    await cache.update(SamplePydanticModel, "test", "age", 2)
    await cache.find_one(SamplePydanticModel, "test", "name") == "test"
    await cache.find_one(SamplePydanticModel, "test", "age") == 2