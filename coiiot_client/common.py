from typing import Dict, Any, NamedTuple, List, Union, Optional, Type
from datetime import datetime
import json
import enum

from . utils import (
    dt_to_ts,
    get_value_or_error,
    get_collection_value,
)


class APIError(Exception):
    pass


class ParseError(APIError):
    pass


class Location(NamedTuple):

    lat: float
    lng: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "lat": self.lat,
            "lng": self.lng,
        }


ValueT = Union[int, str, bool, float, Location, datetime]


class EventTag(NamedTuple):

    id: int
    value: ValueT
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        if isinstance(self.value, Location):
            value = self.value.to_dict()

        elif isinstance(self.value, datetime):
            value = dt_to_ts(self.value)

        else:
            value = self.value

        return {
            "id": self.id,
            "value": value,
            "timestamp": dt_to_ts(self.timestamp),
        }


class EventMessage(NamedTuple):

    tags: List[EventTag]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tags": [tag.to_dict() for tag in self.tags],
        }

    def dumps(self) -> str:
        return json.dumps(self.to_dict())


class TagType(NamedTuple):

    id: int
    name: str

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'TagType':
        return TagType(
            id=get_value_or_error(raw, "id", "tag type input", exception),
            name=get_value_or_error(raw, "name", "tag type input", exception),
        )


class Driver(NamedTuple):

    id: int
    name: str
    protocol: Optional[str]

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'Driver':
        return Driver(
            id=get_value_or_error(raw, "id", "driver input", exception),
            name=get_value_or_error(raw, "name", "driver input", exception),
            protocol=raw.get("protocol"),
        )


class Tag(NamedTuple):

    id: int
    name: str
    properties: Dict[str, Any]
    type: TagType
    attrs: Dict[str, Any]
    children: Dict[str, 'Tag']
    driver_config: Dict[str, Any]

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'Tag':
        children = dict()
        for raw_tag in get_collection_value(raw, "children", list):
            tag = Tag.from_dict(raw_tag, exception)
            children[tag.name] = tag

        return Tag(
            id=get_value_or_error(raw, "id", "tag input", exception),
            name=get_value_or_error(raw, "name", "tag input", exception),
            type=TagType.from_dict(
                get_value_or_error(raw, "type", "tag input", exception),
                exception,
            ),
            properties=get_value_or_error(raw, "properties", "tag input", exception),
            attrs=get_collection_value(raw, "attrs", dict),
            children=children,
            driver_config=get_collection_value(raw, "driver_config", dict),
        )


class Device(NamedTuple):

    id: int
    name: str
    driver: Driver
    tag: Tag
    driver_config: Dict[str, Any]
    config_id: Optional[int]

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'Device':
        return Device(
            id=get_value_or_error(raw, "id", "device input", exception),
            name=get_value_or_error(raw, "name", "device input", exception),
            tag=Tag.from_dict(get_value_or_error(raw, "tag", "device input", exception), exception),
            driver_config=get_collection_value(raw, "driver_config", dict),
            driver=Driver.from_dict(
                get_value_or_error(raw, "driver", "device input", exception),
                exception,
            ),
            config_id=raw.get("config_id"),
        )


class Agent(NamedTuple):

    id: int
    name: str
    tag: Tag
    devices: List[Device]
    config_id: Optional[int]

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'Agent':
        return Agent(
            id=get_value_or_error(raw, "id", "agent input", exception),
            config_id=raw.get("config_id"),
            name=get_value_or_error(raw, "name", "agent input", exception),
            tag=Tag.from_dict(
                get_value_or_error(raw, "tag", "agent input", exception),
                exception,
            ),
            devices=[
                Device.from_dict(raw, exception)
                for raw in get_value_or_error(raw, "devices", "agent input", exception)
            ],
        )


@enum.unique
class LogLevel(enum.IntEnum):
    debug = 1
    info  = 2
    warn  = 3
    error = 4
    fatal = 5


class LogRecord(NamedTuple):

    level: LogLevel
    message: str

    def to_dict(self):
        return {
            "level": self.level.value,
            "message": self.message,
        }


@enum.unique
class CommandStatus(enum.Enum):
    new = "new"
    sending = "sending"
    sent = "sent"
    received = "received"
    skipped = "skipped"
    done = "done"
    failed = "failed"

    @staticmethod
    def from_string(value: str, exception: Type[Exception]) -> 'CommandStatus':
        try:
            return CommandStatus(value)
        except ValueError as e:
            raise exception(f"parse command_status failed, value '{value}' is unknown") from e


class Auth:

    def __init__(self, client_id: int, agent_id: int, agent_token: str):
        self._token = agent_token
        self._client_id = client_id
        self._agent_id = agent_id
    
    @property
    def login(self) -> str:
        return f"{self._client_id}_{self._agent_id}"

    @property
    def passw(self) -> str:
        return self._token

    @property
    def agent_id(self) -> int:
        return self._agent_id
