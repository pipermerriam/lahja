import pytest

import curio

from lahja import (
    BaseEvent,
)


class EventTest(BaseEvent):
    pass


class EventUnexpected(BaseEvent):
    pass


class EventInherited(EventTest):
    pass


@pytest.mark.curio
async def test_curio_endpoint_subscribe(endpoint_server, endpoint_client):
    results = []

    endpoint_server.subscribe(EventTest, results.append)

    await endpoint_client.broadcast(EventTest())
    await endpoint_client.broadcast(EventUnexpected())
    await endpoint_client.broadcast(EventInherited())
    await endpoint_client.broadcast(EventTest())

    # enough cycles to allow the server to process the event
    await curio.sleep(0)
    await curio.sleep(0)

    assert len(results) == 2
    assert all(type(event) is EventTest for event in results)
