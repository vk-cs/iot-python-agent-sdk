import json
from typing import Dict, Any, Type, Optional, Callable
from datetime import datetime


def dt_from_ts(ts: int) -> datetime:
    # timestamp comes in microseconds
    return datetime.fromtimestamp(ts / 1000000.0)


def dt_to_ts(dt: datetime) -> int:
    return int(dt.timestamp() * 1000000)


def get_value_or_error(collection: Dict[str, Any], key: str, name: str = "collection", exception: Type[Exception] = KeyError) -> Any:
    value = collection.get(key)
    if value is None:
        raise exception(f'Key "{key}" missing in {name}')

    return value


def get_collection_value(collection: Dict[str, Any], key: str, factory: Callable[[], Any]) -> Any:
    value = collection.get(key)
    if value is not None:
        return value
    return factory()


def trim_prefix(s, prefix) -> str:
    if s.startswith(prefix):
        return s[len(prefix):]
    
    return s
