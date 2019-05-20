import asyncio

import pytest

from lahja.endpoint import Endpoint


@pytest.mark.asyncio
async def test_asyncio_wait_connected_to(endpoint_server, server_config):
    async with Endpoint("client").run() as client:
        assert not client.is_connected_to(endpoint_server.name)

        asyncio.ensure_future(client.connect_to_endpoint(server_config))
        # still not connected until we yield
        assert not client.is_connected_to(endpoint_server.name)
        await client.wait_connected_to(server_config.name)
        assert client.is_connected_to(endpoint_server.name)
