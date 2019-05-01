import pytest

import curio

from conftest import (
    generate_unique_name,
)
from lahja import (
    BaseEvent,
    ConnectionConfig,
)
from lahja.curio.endpoint import (
    CurioEndpoint,
)


class EventTest(BaseEvent):
    def __init__(self, value):
        self.value = value


@pytest.mark.curio
async def test_curio_endpoint_broadcast(ipc_base_path):
    server = CurioEndpoint()
    config = ConnectionConfig.from_name(generate_unique_name(), base_path=ipc_base_path)
    await server.start_serving(config)

    client = CurioEndpoint()
    await client.connect_to_endpoint(config)

    assert client.is_connected_to(config.name)
    event = EventTest('test')

    task = await curio.spawn(server.wait_for, EventTest)

    await client.broadcast(event)

    result = await task.join()
    assert isinstance(result, EventTest)
    assert result.value == 'test'
