from typing import AsyncContextManager
from datetime import datetime
from contextlib import asynccontextmanager
import asyncio

import pytest

import simplejson as json
from hbmqtt.client import MQTTClient as TestClient, QOS_1

import coiiot_client.mqtt as mqtt


@asynccontextmanager
async def create_test_client(loop, topic=None) -> AsyncContextManager[TestClient]:
    test_client = TestClient("tester", loop=loop)
    await test_client.connect(uri="mqtt://localhost:1883")
    if topic is not None:
        await test_client.subscribe([(topic, QOS_1)])

    try:
        yield test_client

    finally:
        await test_client.disconnect()


@pytest.mark.asyncio
async def test_send_event(event_loop):
    topic = "iot/event/fmt/json"
    async with create_test_client(topic=topic, loop=event_loop) as test_client:
        agent = mqtt.Agent(
            id=1,
            config_id=1,
            name="test_agent",
            tag=mqtt.Tag(
                id=1,
                name="test_tag",
                type=mqtt.TagType(
                    id=1,
                    name="test_type",
                ),
                children={},
                properties={},
                driver_config={},
                attrs={},
            ),
            devices=[],
        )

        client = mqtt.Client(
            broker_uri="localhost:1883",
            auth=mqtt.Auth(client_id=100, agent_id=agent.id, agent_token="tok"),
            loop=event_loop,
        )

        await client.run()

        await client.send_event(
            mqtt.EventMessage(
                tags=[
                    mqtt.EventTag(
                        id=1,
                        value=1,
                        timestamp=datetime.fromtimestamp(1e8)
                    )
                ]
            )
        )

        message = await asyncio.wait_for(test_client.deliver_message(), 5)

        assert json.loads(message.data.decode("utf8")) == {
            "tags": [
                {
                    "id": 1,
                    "value": 1,
                    "timestamp": 1e8 * 1e6
                }
            ]
        }

        await client.close()


@pytest.mark.asyncio
async def test_send_agent_command_status(event_loop):
    agent_id = 1
    topic = f"iot/cmd/agent/{agent_id}/status/fmt/json"
    async with create_test_client(topic=topic, loop=event_loop) as test_client:
        agent = mqtt.Agent(
            id=agent_id,
            config_id=1,
            name="test_agent",
            tag=mqtt.Tag(
                id=1,
                name="test_tag",
                type=mqtt.TagType(
                    id=1,
                    name="test_type",
                ),
                children={},
                properties={},
                driver_config={},
                attrs={},
            ),
            devices=[],
        )

        client = mqtt.Client(
            broker_uri="localhost:1883",
            auth=mqtt.Auth(client_id=100, agent_id=agent.id, agent_token="tok"),
            loop=event_loop,
        )

        await client.run()

        await client.send_agent_command_status(
            mqtt.CommandStatusMessage(
                id="some_command",
                status=mqtt.CommandStatus.done,
                reason=None,
                timestamp=datetime.fromtimestamp(1e8),
            )
        )

        message = await asyncio.wait_for(test_client.deliver_message(), 5)

        assert json.loads(message.data.decode("utf8")) == {
            "id": "some_command",
            "status": "done",
            "reason": None,
            "timestamp": 1e8 * 1e6,
        }

        await client.close()


@pytest.mark.asyncio
async def test_send_device_command_status(event_loop):
    device_id = 100
    topic = f"iot/cmd/device/{device_id}/status/fmt/json"
    async with create_test_client(topic=topic, loop=event_loop) as test_client:
        agent = mqtt.Agent(
            id=1,
            config_id=1,
            name="test_agent",
            tag=mqtt.Tag(
                id=1,
                name="test_tag",
                type=mqtt.TagType(
                    id=1,
                    name="test_type",
                ),
                children={},
                properties={},
                driver_config={},
                attrs={},
            ),
            devices=[
                mqtt.Device(
                    id=device_id,
                    config_id=1,
                    name="test_device",
                    tag=mqtt.Tag(
                        id=2,
                        name="event_tag",
                        type=mqtt.TagType(
                            id=1,
                            name="event",
                        ),
                        children={},
                        properties={},
                        driver_config={},
                        attrs={},
                    ),
                    driver_config={},
                    driver=mqtt.Driver(
                        id=3,
                        name="test_driver",
                        protocol="mqtt",
                    )
                )
            ],
        )

        client = mqtt.Client(
            broker_uri="localhost:1883",
            auth=mqtt.Auth(client_id=100, agent_id=agent.id, agent_token="tok"),
            loop=event_loop,
        )

        await client.run()

        await client.send_device_command_status(
            device_id,
            mqtt.CommandStatusMessage(
                id="some_command",
                status=mqtt.CommandStatus.done,
                reason=None,
                timestamp=datetime.fromtimestamp(1e8),
            )
        )

        message = await asyncio.wait_for(test_client.deliver_message(), 5)

        assert json.loads(message.data.decode("utf8")) == {
            "id": "some_command",
            "status": "done",
            "reason": None,
            "timestamp": 1e8 * 1e6,
        }

        await client.close()


@pytest.mark.asyncio
async def test_send_logs(event_loop):
    topic = "iot/log/fmt/json"
    async with create_test_client(topic=topic, loop=event_loop) as test_client:
        agent = mqtt.Agent(
            id=1,
            config_id=1,
            name="test_agent",
            tag=mqtt.Tag(
                id=1,
                name="test_tag",
                type=mqtt.TagType(
                    id=1,
                    name="test_type",
                ),
                children={},
                properties={},
                driver_config={},
                attrs={},
            ),
            devices=[],
        )

        client = mqtt.Client(
            broker_uri="localhost:1883",
            auth=mqtt.Auth(client_id=100, agent_id=agent.id, agent_token="tok"),
            loop=event_loop,
        )

        await client.run()

        await client.send_logs(
            [
                mqtt.LogRecord(
                    level=1,
                    message="first",
                ),
                mqtt.LogRecord(
                    level=2,
                    message="second",
                )
            ]
        )

        message = await asyncio.wait_for(test_client.deliver_message(), 5)

        assert json.loads(message.data.decode("utf8")) == [
            {
                "level": 1,
                "message": "first",
            },
            {
            "level": 2,
            "message": "second"
            },
        ]

        await client.close()


@pytest.mark.asyncio
async def test_receive_incoming_commands(event_loop):
    async with create_test_client(loop=event_loop) as test_client:
        agent = mqtt.Agent(
            id=1,
            config_id=1,
            name="test_agent",
            tag=mqtt.Tag(
                id=1,
                name="test_tag",
                type=mqtt.TagType(
                    id=1,
                    name="test_type",
                ),
                children={},
                properties={},
                driver_config={},
                attrs={},
            ),
            devices=[],
        )

        client = mqtt.Client(
            broker_uri="localhost:1883",
            auth=mqtt.Auth(client_id=100, agent_id=agent.id, agent_token="tok"),
            loop=event_loop,
        )

        await client.run()

        await test_client.publish(
            f"iot/cmd/agent/{agent.id}/fmt/json",
            json.dumps({
                "command": {
                    "id": "some_command",
                    "tags": [
                        {
                            "id": 1,
                            "value": True,
                        }
                    ],
                    "timestamp": 1577826000000000,
                },
                "devices": [],
            }).encode("utf8"),
            qos=QOS_1
        )

        cmd = await asyncio.wait_for(client.incoming_commands().__anext__(), 5)

        assert cmd == mqtt.CommandMessage(
            command = mqtt.Command(
                id="some_command",
                tags=[
                    mqtt.CommandTag(
                        id=1,
                        value=True,
                    )
                ],
                timestamp=datetime(2020, 1, 1),
            ),
            devices = [],
        )

        await client.close()
