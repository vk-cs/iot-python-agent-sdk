from typing import List, NamedTuple, Union, Dict, Any, AsyncIterator, Optional, Type
from datetime import datetime
import uuid
import asyncio

import simplejson as json
from hbmqtt.client import MQTTClient as HBClient
from hbmqtt.mqtt.constants import QOS_1


from . utils import (
    dt_to_ts,
    dt_from_ts,
    get_value_or_error,
    trim_prefix,
)

from . common import (
    LogRecord,
    LogLevel,
    EventMessage,
    EventTag,
    ValueT,
    Location,
    TagType,
    Tag,
    Driver,
    Device,
    Agent,
    CommandStatus,
    Auth,

    ParseError,
    APIError,
)


class ImproperlyCommandFormatError(ParseError):
    pass


class MQTTError(APIError):
    pass


class SubscriptionError(MQTTError):
    pass


class CommandTag(NamedTuple):

    id: int
    value: ValueT

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'CommandTag':
        return CommandTag(
            id=get_value_or_error(raw, "id", "command tag input", exception),
            value=get_value_or_error(raw, "value", "command tag input", exception),
        )


class Command(NamedTuple):

    id: str
    tags: List[CommandTag]
    timestamp: datetime

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'Command':
        cmd_id = get_value_or_error(raw, "id", "command input", exception)
        tags = get_value_or_error(raw, "tags", "command input", exception)
        timestamp = get_value_or_error(raw, "timestamp", "command input", exception)

        return Command(
            id=cmd_id,
            tags=[
                CommandTag.from_dict(tag_value, exception)
                for tag_value in tags
            ],
            timestamp=dt_from_ts(timestamp),
        )


class DeviceCommand(NamedTuple):

    device_id: int
    command: Command

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'DeviceCommand':
        device_id = get_value_or_error(raw, "device_id", "device_command input", exception)
        cmd = get_value_or_error(raw, "command", "device_command input", exception)

        return DeviceCommand(
            device_id=device_id,
            command=Command.from_dict(cmd, exception),
        )


class CommandMessage(NamedTuple):
    command: Optional[Command]
    devices: List[DeviceCommand]

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'CommandMessage':
        cmd = raw.get("command")
        if cmd is not None:
            cmd = Command.from_dict(cmd, exception)
        
        devices = get_value_or_error(raw, "devices", "command message input", exception)

        return CommandMessage(
            command=cmd,
            devices=[DeviceCommand.from_dict(dev, exception) for dev in devices],
        )

    @staticmethod
    def loads(raw: str, exception: Type[Exception]) -> 'CommandMessage':
        return CommandMessage.from_dict(json.loads(raw), exception)


class CommandStatusMessage(NamedTuple):

    id: str
    status: CommandStatus
    timestamp: datetime
    reason: Union[str, None] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "reason": self.reason,
            "timestamp": dt_to_ts(self.timestamp),
        }

    def dumps(self) -> str:
        return json.dumps(self.to_dict())


class Client(object):

    def __init__(self, broker_uri: str, auth: Auth, loop=asyncio.get_event_loop()):
        self._loop = loop
        base_uri = trim_prefix(broker_uri, "mqtt://")
        self._broker_uri = f"mqtt://{auth.login}:{auth.passw}@{base_uri}"
        self._client = HBClient(
            str(uuid.uuid4()),
            loop=self._loop,
            config={
                "auto_reconnect": False,
            }
        )
        self._auth = auth

    async def run(self):
        await self._client.connect(uri=self._broker_uri)
        ret_code = await self._client.subscribe([
            (f"iot/cmd/agent/{self._auth.agent_id}/fmt/json", QOS_1)
        ])

        # Return code for subscription errors is 0x80. See details here:
        # https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718068
        if ret_code == 0x80:
            raise SubscriptionError

    async def close(self):
        await self._client.disconnect()

    async def send_event(self, msg: EventMessage):
        await self._client.publish(
            "iot/event/fmt/json",
            msg.dumps().encode("utf8"),
            qos=QOS_1
        )

    async def send_agent_command_status(self, msg: CommandStatusMessage):
        await self._client.publish(
            f"iot/cmd/agent/{self._auth.agent_id}/status/fmt/json",
            msg.dumps().encode("utf8"),
            qos=QOS_1
        )
    
    async def send_device_command_status(self, device_id: int, msg: CommandStatusMessage):
        await self._client.publish(
            f"iot/cmd/device/{device_id}/status/fmt/json",
            msg.dumps().encode("utf8"),
            qos=QOS_1
        )

    async def send_logs(self, records: List[LogRecord]):
        await self._client.publish(
            "iot/log/fmt/json",
            json.dumps(records).encode("utf8"),
            qos=QOS_1
        )

    async def incoming_commands(self) -> AsyncIterator[CommandMessage]:
        while True:
            message = await self._client.deliver_message()
            yield CommandMessage.loads(message.data.decode("utf8"), ImproperlyCommandFormatError)
