import asyncio

import pytest

from lahja import AsyncioEndpoint, ConnectionConfig


@pytest.mark.asyncio
async def test_connect_to_endpoint(ipc_base_path):
    config = ConnectionConfig.from_name("server", base_path=ipc_base_path)
    async with AsyncioEndpoint.serve(config) as server:
        async with AsyncioEndpoint("client").run() as client:
            assert not client.is_connected_to(server.name)
            asyncio.ensure_future(client.connect_to_endpoint(config))
            # still not connected because we haven't yielded
            await asyncio.wait_for(
                client.wait_until_connected_to(server.name), timeout=0.1
            )
            assert client.is_connected_to(server.name)
