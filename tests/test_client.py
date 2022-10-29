

from ridant.main import RidantCache
from pydantic import BaseModel
import pytest
from redis import Redis

class SamplePydanticModel(BaseModel):        
    name: str
    age: int
    
def test_object_initialization(return_connection_pool_for_sync_redis):
    cache_object_no_hash = RidantCache(redis_connection_pool=return_connection_pool_for_sync_redis)
    with pytest.raises(ValueError):
        cache_object_no_hash.redis_hashed
        
    cache_object_hash = RidantCache(redis_connection_pool=return_connection_pool_for_sync_redis, redis_database_for_hash=1)
    assert isinstance(cache_object_hash.redis, Redis)
    assert isinstance(cache_object_hash.redis_hashed, Redis)
    assert isinstance(cache_object_no_hash.redis, Redis)


def test_cache_item(return_connection_pool_for_sync_redis):
    cache = RidantCache(redis_connection_pool=return_connection_pool_for_sync_redis)
    cache.cache(SamplePydanticModel(name="test", age=1), "test")
    assert cache.find_one(SamplePydanticModel, "test") == SamplePydanticModel(name="test", age=1)
    
def test_delete_cache_item(return_connection_pool_for_sync_redis):
    cache = RidantCache(redis_connection_pool=return_connection_pool_for_sync_redis)
    cache.cache(SamplePydanticModel(name="test", age=1), "test")
    assert cache.find_one(SamplePydanticModel, "test") == SamplePydanticModel(name="test", age=1)
    assert cache.delete(SamplePydanticModel, "test") == True
    assert cache.find_one(SamplePydanticModel, "test") == None
    
    
def test_hash_set_cache_item(return_connection_pool_for_sync_redis):
    cache = RidantCache(redis_connection_pool=return_connection_pool_for_sync_redis, redis_database_for_hash=1)
    cache.cache(SamplePydanticModel(name="test", age=1), "test", hash=True)
    cache.find_one(SamplePydanticModel, "test", "name") == "test"
    cache.find_one(SamplePydanticModel, "test", "age") == 1

def test_hash_update_cache_item(return_connection_pool_for_sync_redis):
    cache = RidantCache(redis_connection_pool=return_connection_pool_for_sync_redis, redis_database_for_hash=1 )
    cache.cache(SamplePydanticModel(name="test", age=1), "test", hash=True)
    cache.update(SamplePydanticModel, "test", "age", 2)
    cache.find_one(SamplePydanticModel, "test", "name") == "test"
    cache.find_one(SamplePydanticModel, "test", "age") == 2
    
async def test_hash_update_cache_item(return_connection_pool_for_sync_redis):
    cache = RidantCache(redis_connection_pool=return_connection_pool_for_sync_redis, redis_database_for_hash=1 )
    cache.cache_by_group("testing-group", "sample-uid", "coolValue")
    assert cache.find_one_by_group("testing-group", "sample-uid") == "coolValue"
    cache.cache_by_group("testing-group", "sample-uid", 2)
    assert cache.find_one_by_group("testing-group", "sample-uid") == "2"
