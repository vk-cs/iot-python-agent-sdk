from unittest.mock import patch
from datetime import datetime

import pytest

import coiiot_client.http as http
import coiiot_client.utils as utils


class MockResponse:
    def __init__(self, text, status):
        self._text = text
        self.status = status

    async def text(self):
        return self._text

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __aenter__(self):
        return self


@pytest.mark.asyncio
async def test_should_catch_http_error(event_loop):
    with patch('aiohttp.ClientSession.get') as mock:

        mock.return_value = MockResponse("", 404)

        auth = http.Auth(1, 10, "")
        client = http.Client("", auth, loop=event_loop)

        not_found_raised = False
        try:
            await client.get_commands()
        except http.NotFoundError:
            not_found_raised = True

        assert not_found_raised


@pytest.mark.asyncio
async def test_get_config(event_loop):
    with patch('aiohttp.ClientSession.get') as mock:

        resp = r"""
{
    "agent":{
        "config_id":1,
        "devices":[
            {
                "config_id":1,
                "driver":{
                    "id":1,
                    "name":"modbus driver",
                    "protocol":"modbus"
                },
                "driver_config":{"key": "value"},
                "id":1,
                "name":"some_device",
                "tag":{
                    "attrs":{"key": "value"},
                    "children":[],
                    "driver_config":{"key": "value"},
                    "id":1,
                    "name":"some_tag",
                    "properties":{
                        "a":"b"
                    },
                    "type":{
                        "id":1,
                        "name":"undefined"
                    }
                }
            }
        ],
        "id":1,
        "name":"some_agent",
        "tag":{
            "attrs":{"key": "value"},
            "children":[],
            "driver_config":{"key": "value"},
            "id":1,
            "name":"some_tag",
            "properties":{
                "a":"b"
            },
            "type":{
                "id":1,
                "name":"undefined"
            }
        }
    },
    "version":"v1"
}
        """


        mock.return_value = MockResponse(resp, 200)

        auth = http.Auth(1, 10, "")
        client = http.Client("", auth, loop=event_loop)

        config = await client.get_config()

        tag = http.Tag(
            id=1,
            name="some_tag",
            type=http.TagType(
                id=1,
                name="undefined",
            ),
            driver_config={
                "key": "value",
            },
            properties={
                "a": "b",
            },
            children={},
            attrs={
                "key": "value",
            }
        )

        assert config == http.Config(
            version="v1",
            agent=http.Agent(
                id=1,
                config_id=1,
                name="some_agent",
                tag=tag,
                devices=[
                    http.Device(
                        id=1,
                        config_id=1,
                        name="some_device",
                        tag=tag,
                        driver_config={
                            "key": "value",
                        },
                        driver=http.Driver(
                            id=1,
                            name="modbus driver",
                            protocol="modbus",
                        )
                    )
                ]
            )
        )


@pytest.mark.asyncio
async def test_get_commands(event_loop):
    with patch('aiohttp.ClientSession.get') as mock:

        resp = r"""
{
  "command": {
    "created_at": 1577826000000000,
    "id": "some-id",
    "reason": "Failed to send command",
    "status": "new",
    "tags": [
      {
        "tag_id": 1,
        "value": true
      }
    ],
    "updated_at": 1577826000000000
  },
  "devices": [
    {
      "command": {
        "created_at": 1577826000000000,
        "id": "some-id",
        "reason": "Failed to send command",
        "status": "new",
        "tags": [
          {
            "tag_id": 1,
            "value": true
          }
        ],
        "updated_at": 1577826000000000
      },
      "device_id": 1
    }
  ]
}
        """


        mock.return_value = MockResponse(resp, 200)

        auth = http.Auth(1, 10, "")
        client = http.Client("", auth, loop=event_loop)

        commands = await client.get_commands()

        cmd = http.CommandExtended(
            id="some-id",
            reason="Failed to send command",
            status=http.CommandStatus.new,
            tags=[
                http.CommandTag(
                    id=1,
                    value=True,
                )
            ],
            created_at=datetime(2020, 1, 1),
            updated_at=datetime(2020, 1, 1),
        )

        assert commands == http.AgentDevicesCommands(
            command=cmd,
            devices=[
                http.DeviceCommand(
                    command=cmd,
                    device_id=1,
                )
            ]
        )


@pytest.mark.asyncio
async def test_get_device_versioned_config(event_loop):
    with patch('aiohttp.ClientSession.get') as mock:

        resp = r"""
{
  "created_at": 1577826000000000,
  "device_config": {"key": "value"},
  "device_id": 1,
  "id": 1
}
        """


        mock.return_value = MockResponse(resp, 200)

        auth = http.Auth(1, 10, "")
        client = http.Client("", auth, loop=event_loop)

        config = await client.get_device_versioned_config(10)

        assert config == http.VersionedDeviceConfig(
            created_at=datetime(2020, 1, 1),
            device_config={
                "key": "value",
            },
            device_id=1,
            id=1,
        )

