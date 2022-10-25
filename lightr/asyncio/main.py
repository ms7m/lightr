import typing
from lightr.utils.convert_model_to_string_key import get_name_from_model
from loguru import logger
from redis.asyncio import Redis, ConnectionPool
from pydantic import BaseModel
from collections.abc import Awaitable, Coroutine

import json

if typing.TYPE_CHECKING:
    from odmantic import Model

ModelPassed = typing.TypeVar("ModelPassed", BaseModel, "Model")


class PydanticRedisCaching(object):
    def __init__(
        self,
        redis_connection_pool: typing.Optional[ConnectionPool] = None,
        redis_host: typing.Optional[str] = None,
        redis_port: typing.Optional[int] = None,
        redis_database: typing.Optional[int] = None,
        **kwargs,
    ) -> None:
        if redis_connection_pool is None:
            logger.warning(
                "No redis connection pool provided, this is recommended. Using arguments provided (if any)"
            )
            if redis_host is None:
                logger.warning("No redis host provided, using default: localhost")
                redis_host = "localhost"
            if redis_port is None:
                logger.warning("No redis port provided, using default: 6379")
                redis_port = 6379
            if redis_database is None:
                logger.warning("No redis database provided, using default: 0")
                redis_host = 0

            redis_host, redis_port, redis_database = (
                redis_host,
                redis_port,
                redis_database,
            )
        else:
            logger.debug("Using redis connection pool provided")

        self._redis_connection = Redis(
            connection_pool=redis_connection_pool,
        )

    async def _get(self, key_name_provided: str) -> Coroutine[bytes]:
        return await self._redis_connection.get(key_name_provided)

    async def _get_all(
        self, key_name_provided: str
    ) -> Coroutine[typing.Iterator[typing.Any]]:
        return await self._redis_connection.scan_iter(key_name_provided)

    async def _cache(
        self,
        key_name_provided: str,
        value_provided: typing.Union[BaseModel, typing.Any],
        extra_redis_arguments: typing.Optional[dict] = {},
    ) -> typing.Optional[typing.Coroutine]:
        if isinstance(value_provided, BaseModel):
            value_provided = value_provided.json()
        return await self._redis_connection.set(
            key_name_provided, value_provided, **extra_redis_arguments
        )

    async def cache(
        self,
        model: ModelPassed,
        uid: str,
        extra_redis_arguments: typing.Optional[dict] = {},
    ) -> Coroutine[bool]:
        _retrieve_model_name_for_cache = get_name_from_model(model)
        _generated_key_name = [_retrieve_model_name_for_cache, uid]
        return await self._cache(
            ":".join(_generated_key_name), model.json(), extra_redis_arguments
        )

    async def find_one(self, model: ModelPassed, uid: str) -> Coroutine[ModelPassed]:
        _retrieve_model_name_for_cache = get_name_from_model(model)
        _generated_key_name = [_retrieve_model_name_for_cache, uid]

        _fetched_item = await self._get(":".join(_generated_key_name))
        if _fetched_item is None:
            return None

        if not hasattr(model, "parse_raw"):
            logger.warning(
                "Older version of pydantic detected, using json.loads instead of parse_raw"
            )
            return model(**json.loads(_fetched_item.decode("utf-8")))
        return model.parse_raw(_fetched_item)

    async def find(self, model: ModelPassed) -> Awaitable[typing.Iterator[ModelPassed]]:
        raise NotImplementedError
