import re
import typing
from loguru import logger

ODMANTIC_AVAILABLE = False

try:
    from odmantic import Model

    ODMANTIC_AVAILABLE = True
except Exception:
    pass

from pydantic import BaseModel


def to_snake_case(s: str) -> str:
    tmp = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", tmp).lower()


def get_name_from_model(model: typing.Union["Model", "BaseModel"]) -> str:
    if hasattr(model.__config__, "cacheable_group_name"):
        return model.__config__.cacheable_group_name
    else:
        logger.warning(f"Model {model} has no cacheable_group_name attribute -- trying to determine from the class itself.")
        if ODMANTIC_AVAILABLE:
            if isinstance(model, Model):
                return to_snake_case(model.__collection__)
        
        if hasattr(model, "__name__"):
            return to_snake_case(model.__name__)
        
        if hasattr(model, "__class__"):
            return to_snake_case(model.__class__.__name__)
        
        raise ValueError(f"Could not determine name from model {model} -- please provide a cacheable_group_name attribute to the model's config.")
