

from ridant.asyncio.main import RidantCache
from pydantic import BaseModel
import pytest
from redis.asyncio import Redis

class SamplePydanticModel(BaseModel):        
    name: str
    age: int
    
async def test_object_initialization(return_connection_pool_for_async_redis):
    cache_object_no_hash = RidantCache(redis_connection_pool=return_connection_pool_for_async_redis)
    with pytest.raises(ValueError):
        cache_object_no_hash.redis_hashed
        
    cache_object_hash = RidantCache(redis_connection_pool=return_connection_pool_for_async_redis, redis_database_for_hash=1)
    assert isinstance(cache_object_hash.redis, Redis)
    assert isinstance(cache_object_hash.redis_hashed, Redis)
    assert isinstance(cache_object_no_hash.redis, Redis)


async def test_cache_item(return_connection_pool_for_async_redis):
    cache = RidantCache(redis_connection_pool=return_connection_pool_for_async_redis)
    await cache.cache(SamplePydanticModel(name="test", age=1), "test")
    assert await cache.find_one(SamplePydanticModel, "test") == SamplePydanticModel(name="test", age=1)
    
async def test_delete_cache_item(return_connection_pool_for_async_redis):
    cache = RidantCache(redis_connection_pool=return_connection_pool_for_async_redis)
    await cache.cache(SamplePydanticModel(name="test", age=1), "test")
    await cache.cache_by_group("testing-group", "sample-uid", "coolValue")
    assert await cache.find_one(SamplePydanticModel, "test") == SamplePydanticModel(name="test", age=1)
    assert await cache.find_one_by_group("testing-group", "sample-uid") == "coolValue"
    assert await cache.delete(SamplePydanticModel, "test") == True
    assert await cache.find_one(SamplePydanticModel, "test") == None
    assert await cache.delete_by_group("testing-group", "sample-uid") == True
    assert await cache.find_one_by_group("testing-group", "sample-uid") == None
    
    
async def test_hash_set_cache_item(return_connection_pool_for_async_redis):
    cache = RidantCache(redis_connection_pool=return_connection_pool_for_async_redis, redis_database_for_hash=1)
    await cache.cache(SamplePydanticModel(name="test", age=1), "test", hash=True)
    await cache.find_one(SamplePydanticModel, "test", "name") == "test"
    await cache.find_one(SamplePydanticModel, "test", "age") == 1

async def test_hash_update_cache_item(return_connection_pool_for_async_redis):
    cache = RidantCache(redis_connection_pool=return_connection_pool_for_async_redis, redis_database_for_hash=1 )
    await cache.cache(SamplePydanticModel(name="test", age=1), "test", hash=True)
    await cache.update(SamplePydanticModel, "test", "age", 2)
    await cache.find_one(SamplePydanticModel, "test", "name") == "test"
    await cache.find_one(SamplePydanticModel, "test", "age") == 2
    
    
async def test_hash_update_cache_item(return_connection_pool_for_async_redis):
    cache = RidantCache(redis_connection_pool=return_connection_pool_for_async_redis, redis_database_for_hash=1 )
    await cache.cache_by_group("testing-group", "sample-uid", "coolValue")
    assert await cache.find_one_by_group("testing-group", "sample-uid") == "coolValue"
    await cache.cache_by_group("testing-group", "sample-uid", 2)
    assert await cache.find_one_by_group("testing-group", "sample-uid") == "2"
    
    