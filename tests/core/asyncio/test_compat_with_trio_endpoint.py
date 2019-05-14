import asyncio
import multiprocessing
import uuid

import pytest
import trio

from lahja import BaseEvent
from lahja.endpoint import ConnectionConfig
from lahja.endpoint import Endpoint as LegacyEndpoint
from lahja.trio.endpoint import TrioEndpoint


class EventTest(BaseEvent):
    def __init__(self, value):
        self.value = value


async def _do_trio_client_endpoint(name, ipc_path):
    config = ConnectionConfig(name, ipc_path)
    async with TrioEndpoint(name + "-client").run() as client:
        await client.connect_to_endpoint(config)

        assert client.is_connected_to(name)
        event = EventTest("test")

        await client.broadcast(event)


@pytest.mark.asyncio
async def test_legacy_endpoint_serving_trio_endpoint(ipc_base_path):
    config = ConnectionConfig.from_name(str(uuid.uuid4()), ipc_base_path)

    async with LegacyEndpoint.serve(config) as server:
        fut = asyncio.ensure_future(server.wait_for(EventTest))

        proc = multiprocessing.Process(
            target=trio.run,
            args=(_do_trio_client_endpoint, server.name, server.ipc_path),
        )
        proc.start()
        proc.join()

        result = await fut
        assert isinstance(result, EventTest)
        assert result.value == "test"
