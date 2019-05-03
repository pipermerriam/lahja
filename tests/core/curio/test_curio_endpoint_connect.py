import pytest

from conftest import (
    generate_unique_name,
)
from lahja import (
    ConnectionAttemptRejected,
    ConnectionConfig,
)
from lahja.curio.endpoint import (
    CurioEndpoint,
    serve,
)


@pytest.mark.curio
async def test_connecting_to_other_curio_endpoint(ipc_base_path):
    config = ConnectionConfig.from_name(generate_unique_name(), base_path=ipc_base_path)
    async with serve(config):
        async with CurioEndpoint('client') as client:
            await client.connect_to_endpoint(config)

            assert client.is_connected_to(config.name)


@pytest.mark.curio
async def test_curio_duplicate_endpoint_connection_is_error(ipc_base_path):
    config = ConnectionConfig.from_name(generate_unique_name(), base_path=ipc_base_path)
    async with serve(config):
        async with CurioEndpoint('client') as client:
            await client.connect_to_endpoint(config)

            assert client.is_connected_to(config.name)

            with pytest.raises(ConnectionAttemptRejected):
                await client.connect_to_endpoint(config)
