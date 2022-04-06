import base64
from datetime import datetime
from typing import Dict, Any, NamedTuple, List, Union, Optional, Type

import aiohttp
import asyncio

import simplejson as json

from . utils import (
    dt_to_ts,
    dt_from_ts,
    get_value_or_error,
    get_collection_value,
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


class ImproperlyConfigurationError(ParseError):
    pass


class HTTPError(APIError):
    pass


class BadParamsError(HTTPError):
    pass


class UnauthorizedError(HTTPError):
    pass


class NotFoundError(HTTPError):
    pass


class InternalServerError(HTTPError):
    pass


class Config(NamedTuple):

    agent: Agent
    version: str

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'Config':
        agent = get_value_or_error(raw, "agent", "config input", exception)
        return Config(
            version=get_value_or_error(raw, "version", "config input", exception),
            agent=Agent.from_dict(agent, exception),
        )

    @staticmethod
    def loads(raw: str, exception: Type[Exception]) -> 'Config':
        return Config.from_dict(json.loads(raw), exception)


class CommandTag(NamedTuple):

    id: int
    value: ValueT

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'CommandTag':
        return CommandTag(
            id=get_value_or_error(raw, "tag_id", "command tag input", exception),
            value=get_value_or_error(raw, "value", "command tag input", exception),
        )


class CommandExtended(NamedTuple):

    id: str
    tags: List[CommandTag]
    created_at: datetime
    updated_at: datetime
    status: CommandStatus
    reason: Optional[str]

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'CommandExtended':
        cmd_id = get_value_or_error(raw, "id", "command extended input", exception)
        tags = get_value_or_error(raw, "tags", "command extended input", exception)
        created_at = get_value_or_error(raw, "created_at", "command extended input", exception)
        updated_at = get_value_or_error(raw, "updated_at", "command extended input", exception)
        status = get_value_or_error(raw, "status", "command extended input", exception)

        return CommandExtended(
            id=cmd_id,
            tags=[
                CommandTag.from_dict(tag_value, exception)
                for tag_value in tags
            ],
            created_at=dt_from_ts(created_at),
            updated_at=dt_from_ts(updated_at),
            status=CommandStatus.from_string(status, exception),
            reason=raw.get("reason"),
        )


class DeviceCommand(NamedTuple):

    device_id: int
    command: CommandExtended

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'DeviceCommand':
        device_id = get_value_or_error(raw, "device_id", "device_command input", exception)
        cmd = get_value_or_error(raw, "command", "device_command input", exception)

        return DeviceCommand(
            device_id=device_id,
            command=CommandExtended.from_dict(cmd, exception),
        )


class AgentDevicesCommands(NamedTuple):

    command: Optional[CommandExtended]
    devices: List[DeviceCommand]

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'AgentDevicesCommands':
        cmd = raw.get("command")
        if cmd is not None:
            cmd = CommandExtended.from_dict(cmd, exception)
        
        devices = get_value_or_error(raw, "devices", "agent_devices_commands input", exception)

        return AgentDevicesCommands(
            command=cmd,
            devices=[DeviceCommand.from_dict(dev, exception) for dev in devices],
        )

    @staticmethod
    def loads(raw: str, exception: Type[Exception]) -> 'AgentDevicesCommands':
        return AgentDevicesCommands.from_dict(json.loads(raw), exception)


class VersionedDeviceConfig(NamedTuple):

    id: int
    device_id: int
    created_at: Optional[datetime]
    device_config: Dict[str, Any]

    @staticmethod
    def from_dict(raw: Dict[str, Any], exception: Type[Exception]) -> 'VersionedDeviceConfig':  
        id_ = get_value_or_error(raw, "id", "versioned_device_config input", exception)
        device_id = get_value_or_error(raw, "device_id", "versioned_device_config input", exception)
        
        created_at = raw.get("created_at")
        if created_at is not None:
            created_at = dt_from_ts(created_at)

        return VersionedDeviceConfig(
            id=id_,
            device_id=device_id,
            created_at=created_at,
            device_config=get_collection_value(raw, "device_config", dict),
        )

    @staticmethod
    def loads(raw: str, exception: Type[Exception]) -> 'VersionedDeviceConfig':
        return VersionedDeviceConfig.from_dict(json.loads(raw), exception)


class CommandStatusMessage(NamedTuple):

    status: CommandStatus
    timestamp: Optional[datetime] = None
    reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "status": self.status.value,
            "reason": self.reason,
            "timestamp": None,
        }

        if self.timestamp is not None:
            result["timestamp"] = dt_to_ts(self.timestamp)
        
        return result


class Client(object):

    def __init__(self, base_url: str, auth: Auth, timeout=20, loop=asyncio.get_event_loop()):
        self._base_url = base_url
        self._loop = loop
        self._auth = auth
        self._timeout = timeout

    async def _do_request(self, method, url, **kwargs) -> Dict[str, Any]:
        async with aiohttp.ClientSession(loop=self._loop) as session:
            async with getattr(session, method)(
                url,
                timeout=self._timeout,
                auth=aiohttp.BasicAuth(
                    login=self._auth.login,
                    password=self._auth.passw,
                ),
                **kwargs,
            ) as resp:
                response = await resp.text()
                if resp.status == 200:
                    return {"status": resp.status, "text": response}

                if resp.status == 400:
                    raise BadParamsError(response)

                if resp.status == 401:
                    raise UnauthorizedError(response)

                if resp.status == 404:
                    raise NotFoundError(response)

                if resp.status == 500:
                    raise InternalServerError(response)    

                raise HTTPError(f"'{url}' returns unexpected status code '{resp.status}' with body '{response}' ")

    async def get_config(self, version: Union[str, None] = None) -> Config:
        params = {}
        if version is not None:
            params = {"version": version}
        
        resp = await self._do_request("get",
                                        f'{ self._base_url }/v1/agents/config',
                                        params=params,
                                        )
        
        return Config.loads(resp['text'], ImproperlyConfigurationError)

    async def send_agent_command_status(self, command_id: str, status: CommandStatusMessage):
        await self._do_request(
            "patch",
            f'{self._base_url}/v1/agents/{self._auth.agent_id}/commands/{command_id}/status',
            json=status.to_dict(),
        )

    async def get_commands(self) -> AgentDevicesCommands:
        resp = await self._do_request(
            "get",
            f'{self._base_url}/v1/commands',
        )

        return AgentDevicesCommands.loads(resp['text'], ParseError)

    async def get_device_versioned_config(self, version_id: int) -> VersionedDeviceConfig:    
 
        resp = await self._do_request(
            "get",
            f'{self._base_url}/v1/devices/config/{version_id}',
        )

        return VersionedDeviceConfig.loads(resp['text'], ParseError)

    async def send_device_command_status(self, device_id: int, command_id: str, status: CommandStatusMessage):    
        await self._do_request(
            "patch",
            f'{self._base_url}/v1/devices/{device_id}/commands/{command_id}/status',
            json=status.to_dict(),
        )

    async def send_event(self, msg: EventMessage):
        await self._do_request("post",
                                 f'{ self._base_url }/v1/events',
                                 json=msg.to_dict(),
                                )

    async def send_logs(self, records: List[LogRecord]):
        await self._do_request("post",
                                 f'{ self._base_url }/v1/logs',
                                 json=[rec.to_dict() for rec in records],
                                )
