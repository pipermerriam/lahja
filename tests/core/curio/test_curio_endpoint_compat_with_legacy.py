import asyncio
import multiprocessing
import uuid

import curio

import pytest

from lahja import (
    BaseEvent,
)
from lahja.endpoint import (
    ConnectionConfig,
    Endpoint as LegacyEndpoint,
)
from lahja.curio.endpoint import (
    CurioEndpoint,
)


class EventTest(BaseEvent):
    def __init__(self, value):
        self.value = value


def run_asyncio(coro, name, ipc_path):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(coro(name, ipc_path))
    loop.close()


async def _do_legacy_client_endpoint(name, ipc_path):
    client = LegacyEndpoint()
    await client.start_serving(ConnectionConfig.from_name(name + '-client'))
    await client.connect_to_endpoint(ConnectionConfig(name, ipc_path))

    assert client.is_connected_to(name)
    event = EventTest('test')

    await client.broadcast(event)


@pytest.mark.curio
async def test_curio_endpoint_serving_legacy_endpoint(endpoint_server, endpoint_server_config):
    task = await curio.spawn(endpoint_server.wait_for, EventTest)

    name = endpoint_server_config.name
    path = endpoint_server_config.path

    proc = multiprocessing.Process(
        target=run_asyncio,
        args=(_do_legacy_client_endpoint, name, path),
    )
    proc.start()

    result = await task.join()
    assert isinstance(result, EventTest)
    assert result.value == 'test'

    proc.join()


async def _do_curio_client_endpoint(name, ipc_path):
    config = ConnectionConfig(name, ipc_path)
    async with CurioEndpoint(name + '-client') as client:
        await client.connect_to_endpoint(config)

        assert client.is_connected_to(name)
        event = EventTest('test')

        await client.broadcast(event)


@pytest.mark.asyncio
async def test_legacy_endpoint_serving_curio_endpoint(ipc_base_path):
    config = ConnectionConfig.from_name(str(uuid.uuid4()), ipc_base_path)
    server = LegacyEndpoint()
    await server.start_serving(config)

    with server:
        fut = asyncio.ensure_future(server.wait_for(EventTest))

        proc = multiprocessing.Process(
            target=curio.run,
            args=(_do_curio_client_endpoint, server.name, server.ipc_path),
        )
        proc.start()
        proc.join()

        result = await fut
        assert isinstance(result, EventTest)
        assert result.value == 'test'
