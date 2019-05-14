import asyncio
import multiprocessing

import pytest
import trio

from lahja.asyncio import AsyncioEndpoint
from lahja.common import BaseEvent, ConnectionConfig


class EventTest(BaseEvent):
    def __init__(self, value):
        self.value = value


def run_asyncio(coro, *args):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(coro(*args))
    loop.close()


async def _do_legacy_client_endpoint(name, ipc_path, ready):
    config = ConnectionConfig(name, ipc_path)
    # TODO: why does this only work if we `serve`?
    async with AsyncioEndpoint.serve(
        ConnectionConfig.from_name(name + "client")
    ) as client:
        await client.connect_to_endpoint(config)

        assert client.is_connected_to(name)
        event = EventTest("test")

        await client.broadcast(event)


@pytest.mark.skip(reason="Legacy still doesn't expect two way connections")
@pytest.mark.trio
async def test_trio_endpoint_serving_legacy_endpoint(
    endpoint_server, endpoint_server_config
):
    ready = multiprocessing.Event()

    async def _do_wait_for():
        result = await endpoint_server.wait_for(EventTest)
        assert isinstance(result, EventTest)
        assert result.value == "test"

    async with trio.open_nursery() as nursery:
        nursery.start_soon(_do_wait_for)

        name = endpoint_server_config.name
        path = endpoint_server_config.path

        proc = multiprocessing.Process(
            target=run_asyncio, args=(_do_legacy_client_endpoint, name, path, ready)
        )
        proc.start()
        await trio.run_sync_in_worker_thread(proc.join)
