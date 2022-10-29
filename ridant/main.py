import typing
from ridant.utils.convert_model_to_string_key import get_name_from_model
from loguru import logger
from redis import Redis, ConnectionPool
from pydantic import BaseModel
from collections.abc import Awaitable, Coroutine

import json

if typing.TYPE_CHECKING:
    from odmantic import Model

ModelPassed = typing.TypeVar("ModelPassed", BaseModel, "Model")


class RidantCache(object):
    def __init__(
        self,
        redis_connection_pool: typing.Optional[ConnectionPool] = None,
        redis_host: typing.Optional[str] = None,
        redis_port: typing.Optional[int] = None,
        redis_database: typing.Optional[int] = None,
        default_hset_uid_key: typing.Optional[str] = None,
        use_different_db_for_hash: bool = False,
        redis_database_for_hash: typing.Optional[int] = None,
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

            self.redis_host, self.redis_port, self.redis_database = (
                redis_host,
                redis_port,
                redis_database,
            )
        else:
            logger.debug("Using redis connection pool provided")

        self.default_hset_uid_key = default_hset_uid_key

        if redis_database_for_hash:
            self._redis_connection_hash_only = Redis(
                connection_pool=redis_connection_pool,
                port=redis_port,
                host=redis_host,
                db=redis_database_for_hash
                if redis_database_for_hash
                else redis_database,
            )
            logger.debug(
                f"Redis Hashed Connection Information: <host: {redis_host}, port: {redis_port}, db: {redis_database_for_hash if redis_database_for_hash else redis_database}>"
            )
        else:
            self._redis_connection_hash_only = None
            logger.debug(f"Redis Hashed Connection Information: <none>")

        self._redis_connection = Redis(
            connection_pool=redis_connection_pool,
        )
        logger.debug(
            f"Redis Connection Information: <host: {redis_host}, port: {redis_port}, db: {redis_database}>"
        )

    @property
    def redis(self) -> Redis:
        return self._redis_connection

    @staticmethod
    def generate_key_name(model: typing.Union[ModelPassed, str], uid: str) -> str:
        if isinstance(model, str):
            return f"{model}:{uid}"
        else:
            return ":".join([get_name_from_model(model), uid])

    def _item_be_converted_to_dict(self, item: typing.Any) -> typing.TypeVar("item"):
        if isinstance(self._convert_object_to_safe_redis_type(item), dict):
            return item
        return False

    @property
    def redis_hashed(self) -> Redis:
        if self._redis_connection_hash_only is not None:
            return self._redis_connection_hash_only
        raise ValueError("Hashed redis client is not available.")

    def _get(self, key_name_provided: str) -> bytes:
        return self.redis.get(key_name_provided)

    def _hget(self, key_name_provided: str, attr: str) -> bytes:
        return self.redis_hashed.hget(key_name_provided, attr)

    def _get_all(
        self, key_name_provided: str
    ) -> Coroutine[typing.Iterator[typing.Any]]:
        return self.redis.scan_iter(key_name_provided)

    @staticmethod
    def _convert_object_to_safe_redis_type(val: typing.Union[BaseModel, typing.Any]):
        if hasattr(val, "dict") and callable(val.dict):
            # Just to make sure that all values can be set in redis
            val: dict = json.loads(val.json())
        else:

            if isinstance(val, (int, float, str, bool)):
                return val

            try:
                _attempt_to_convert_to_dict = json.loads(val)
                return _attempt_to_convert_to_dict
            except Exception:
                logger.exception(
                    "Unable to convert to dict (this was a fallback attempt as the the object did not have a .dict() method)"
                )
                raise ValueError(
                    "Unable to convert to dict (this was a fallback attempt as the the object did not have a .dict() method)"
                )

        return val

    def _hash_cache_attribute(
        self,
        attr: str,
        value: typing.Any,
        model: typing.Optional[ModelPassed] = None,
        key_name: typing.Optional[str] = None,
        redis_instance: Redis = None,
        uid: typing.Optional[str] = None,
    ) -> typing.Any:
        if redis_instance is None:
            redis_instance = self.redis_hashed

        if model is None and key_name is not None:
            logger.debug(f"Using provided key name: '{key_name}' for hset")
            return redis_instance.hset(key_name, attr, value)

        if uid is None and self.default_hset_uid_key is None:
            raise ValueError("No uid provided and no default uid key provided")
        else:

            _generated_key_name = [
                get_name_from_model(model),
                uid if uid is not None else self.default_hset_uid_key,
            ]
            _generated_key_name = ":".join(_generated_key_name)

        return redis_instance.hset(_generated_key_name, attr, value)

    def _hash_cache(
        self,
        key_name_provided: str,
        value_provided: typing.Union[BaseModel, typing.Any],
        extra_redis_arguments: typing.Optional[dict] = {},
    ) -> bool:
        _values = self._convert_object_to_safe_redis_type(val=value_provided)
        try:
            with self.redis_hashed.pipeline() as pipe:
                for key, value in _values.items():
                    if isinstance(value, (list, dict)):
                        logger.warning(
                            f"Please avoid using lists or dicts, You will need to overwrite the entire key in order to update. Key: {key}, Value: {value}"
                        )
                        self._hash_cache_attribute(
                            key_name=key_name_provided,
                            attr=key,
                            value=json.dumps(value),
                            redis_instance=pipe,
                        )

                    else:
                        self._hash_cache_attribute(
                            key_name=key_name_provided,
                            attr=key,
                            value=value,
                            redis_instance=pipe,
                        )

                pipe.execute()
        except Exception:
            logger.exception("Unable to cache with hset")
            raise ValueError("Unable to cache with hset")

    def _cache(
        self,
        key_name_provided: str,
        value_provided: typing.Union[BaseModel, typing.Any],
        extra_redis_arguments: typing.Optional[dict] = {},
    ) -> typing.Optional[typing.Coroutine]:
        if isinstance(value_provided, BaseModel):
            value_provided = value_provided.json()
        return self.redis.set(
            key_name_provided, value_provided, **extra_redis_arguments
        )


    def find_one_by_group(
        self, group: str, uid: str
    )-> typing.Optional[str]:
        _res = self._get(
            key_name_provided=self.generate_key_name(model=group, uid=uid)
        )
        if _res and isinstance(_res, bytes):
            return _res.decode("utf-8")
        elif _res is not None:
            return _res
        return None


    def cache_by_group(
        self,
        group: str,
        uid: str,
        value: typing.Union[BaseModel, typing.Any],
        extra_redis_arguments: typing.Optional[dict] = {},
        hash: bool = False,
    ) -> bool:
        _generated_key_name = [group, uid]

        if hash:
            logger.debug(f"hash argument provided, using hset instead of set")

            _can_item_be_hashsed = self._item_be_converted_to_dict(item=value)
            if _can_item_be_hashsed == False:
                raise TypeError("Item passed cannot be broken down and hashed.")

            return self._hash_cache(
                key_name_provided=":".join(_generated_key_name),
                value_provided=_can_item_be_hashsed,
                extra_redis_arguments=extra_redis_arguments,
            )
        else:
            return self._cache(
                ":".join(_generated_key_name),
                self._convert_object_to_safe_redis_type(val=value),
                extra_redis_arguments,
            )

    def cache(
        self,
        model: ModelPassed,
        uid: str,
        extra_redis_arguments: typing.Optional[dict] = {},
        hash: bool = False,
    ) -> bool:
        _retrieve_model_name_for_cache = get_name_from_model(model)
        _generated_key_name = [_retrieve_model_name_for_cache, uid]

        if hash:
            logger.debug(f"hash argument provided, using hset instead of set")
            return self._hash_cache(
                key_name_provided=":".join(_generated_key_name),
                value_provided=model,
                extra_redis_arguments=extra_redis_arguments,
            )
        else:
            return self._cache(
                ":".join(_generated_key_name), model.json(), extra_redis_arguments
            )

    def find_one(
        self,
        model: ModelPassed,
        uid: str,
        specific_attribute: typing.Optional[str] = None,
    ) -> typing.Union[ModelPassed, typing.Any]:

        if specific_attribute:
            logger.debug("Attribute args provided, using hget")
            return self._hget(
                key_name_provided=get_name_from_model(model) + ":" + uid,
                attr=specific_attribute,
            )

        _retrieve_model_name_for_cache = get_name_from_model(model)
        _generated_key_name = [_retrieve_model_name_for_cache, uid]

        _fetched_item = self._get(":".join(_generated_key_name))
        if _fetched_item is None:
            return None

        if not hasattr(model, "parse_raw"):
            logger.warning(
                "Older version of pydantic detected, using json.loads instead of parse_raw"
            )
            return model(**json.loads(_fetched_item.decode("utf-8")))
        return model.parse_raw(_fetched_item)

    def find(self, model: ModelPassed) -> typing.Iterator[ModelPassed]:
        raise NotImplementedError

    def _clear_key(self, key_name_provided: str) -> bool:
        return self.redis.delete(key_name_provided)

    def delete(self, model: ModelPassed, uid: str) -> bool:
        return self._clear_key(":".join([get_name_from_model(model), uid]))

    def update(
        self,
        model: ModelPassed,
        uid: str,
        attribute_to_update: typing.Optional[str],
        attribute_value_to_be_updated_to: typing.Optional[
            typing.Union[str, int, bytes]
        ],
        extra_redis_arguments: typing.Optional[dict] = {},
    ) -> bool:
        if attribute_to_update and attribute_value_to_be_updated_to is not None:
            if isinstance(attribute_value_to_be_updated_to, (list, dict)):
                logger.warning(
                    f"Please avoid using lists or dicts, You will need to overwrite the entire key in order to update. Key: {attribute_to_update}, Value: {attribute_value_to_be_updated_to}"
                )
                return self._hash_cache(
                    key_name_provided=get_name_from_model(model) + ":" + uid,
                    value_provided=json.dumps(attribute_value_to_be_updated_to),
                    extra_redis_arguments=extra_redis_arguments,
                )
            return self._hash_cache_attribute(
                key_name=get_name_from_model(model) + ":" + uid,
                attr=attribute_to_update,
                value=attribute_value_to_be_updated_to,
                # extra_redis_arguments=extra_redis_arguments,
            )
        else:
            logger.warning("If you're not updating a specific attribute, use cache()")
            return self.cache(
                model=model, uid=uid, extra_redis_arguments=extra_redis_arguments
            )
