import asyncio
import aiojobs
import random
import argparse
from datetime import datetime

import coiiot_client.http as http


async def commands_coro(http_client: http.Client, config: http.Config):
    while True:

        commands = await http_client.get_commands()

        print("got commands: ", commands)

        for command in commands.devices:
            # accepting device command
            await http_client.send_device_command_status(
                command.device_id,
                command.command.id,
                http.CommandStatusMessage(
                    status=http.CommandStatus.received,
                    timestamp=datetime.now(),
                ),
            )

            # do some work ...

            # mark device command as done
            await http_client.send_device_command_status(
                command.device_id,
                command.command.id,
                http.CommandStatusMessage(
                    status=http.CommandStatus.done,
                    timestamp=datetime.now(),
                )
            )

        if commands.command is None:
            continue
        
        # accepting agent command
        await http_client.send_agent_command_status(
            config.agent.id,
            commands.command.id,
            http.CommandStatusMessage(
                status=http.CommandStatus.received,
                timestamp=datetime.now(),
            ),
        )

        # do some work ...

        # mark agent command as done
        await http_client.send_agent_command_status(
            config.agent.id,
            commands.command.id,
            http.CommandStatusMessage(
                status=http.CommandStatus.done,
                timestamp=datetime.now(),
            )
        )

        await asyncio.sleep(10)


async def run_example(cli_config):

    auth = http.Auth(
        client_id=cli_config.client_id,
        agent_id=cli_config.agent_id,
        agent_token=cli_config.agent_token,
    )

    http_client = http.Client(base_url=cli_config.http_addr, auth=auth)

    # get latest config
    config = await http_client.get_config()
    
    await http_client.send_event(http.EventMessage(
        tags=[
            http.EventTag(
                id=config.agent.tag.children['$state'].children['$status'].id,
                value="bootstrapping",
                timestamp=datetime.now()
            )
        ]
    ))

    # bootstrap agent ...
    scheduler = await aiojobs.create_scheduler()
    await scheduler.spawn(commands_coro(http_client, config))

    # mark agent is online
    await http_client.send_event(http.EventMessage(
        tags=[
            http.EventTag(
                id=config.agent.tag.children['$state'].children['$status'].id,
                value="online",
                timestamp=datetime.now(),
            ),
            http.EventTag(
                id=config.agent.tag.children['$state'].children['$config'].children['$updated_at'].id,
                value=datetime.now(),
                timestamp=datetime.now(),
            ),
        ]
    ))

    # sending temperature from thermometer of first device
    while True:
        temp = random.randint(20, 30)
        print(f"sending temperature value={temp}")

        await http_client.send_event(http.EventMessage(
            tags=[
                http.EventTag(
                    id=config.agent.devices[0].tag.children["thermometer"].children["temperature"].id,
                    value=temp,
                    timestamp=datetime.now(),
                )
            ]
        ))

        await http_client.send_logs(
            [
                http.LogRecord(
                    level=http.LogLevel.info,
                    message=f"temperature is {temp}"
                )
            ]
        )

        await asyncio.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("http_addr", type=str, help="HTTP base url (e.g. http://domain.com)")
    parser.add_argument("client_id", type=int, help="Client ID")
    parser.add_argument("agent_id", type=int, help="Agent ID")
    parser.add_argument("agent_token", type=str, help="Agent Token")
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_example(args))


if __name__ == "__main__":
    main()
