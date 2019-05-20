import asyncio

import pytest

from conftest import generate_unique_name
from lahja import ConnectionConfig
from lahja.endpoint import Endpoint


@pytest.mark.asyncio
async def test_legacy_server_endpoint_establishes_reverse_connection_to_client(
    ipc_base_path
):
    unique_name = generate_unique_name()
    config = ConnectionConfig.from_name(
        f"server-{unique_name}", base_path=ipc_base_path
    )

    async with Endpoint.serve(config) as server:
        async with Endpoint(f"client-{unique_name}").run() as client:
            await client.connect_to_endpoint(config)

            assert client.is_connected_to(config.name)
            await asyncio.wait_for(
                server.wait_connected_to(f"client-{unique_name}"), timeout=0.1
            )
