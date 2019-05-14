import pytest
import trio

from lahja import BaseEvent


class EventTest(BaseEvent):
    pass


class EventUnexpected(BaseEvent):
    pass


class EventInherited(EventTest):
    pass


@pytest.mark.trio
async def test_trio_endpoint_subscribe(endpoint_server, endpoint_client):
    results = []

    await endpoint_server.subscribe(EventTest, results.append)

    await endpoint_client.broadcast(EventTest())
    await endpoint_client.broadcast(EventUnexpected())
    await endpoint_client.broadcast(EventInherited())
    await endpoint_client.broadcast(EventTest())

    # enough cycles to allow the server to process the event
    await trio.sleep(0.05)

    assert len(results) == 2
    assert all(type(event) is EventTest for event in results)


@pytest.mark.trio
async def test_trio_endpoint_unsubscribe(endpoint_server, endpoint_client):
    results = []

    subscription = await endpoint_server.subscribe(EventTest, results.append)

    await endpoint_client.broadcast(EventTest())
    await endpoint_client.broadcast(EventUnexpected())
    await endpoint_client.broadcast(EventInherited())
    await endpoint_client.broadcast(EventTest())

    # enough cycles to allow the server to process the event
    await trio.sleep(0.05)

    subscription.unsubscribe()

    await endpoint_client.broadcast(EventTest())
    await endpoint_client.broadcast(EventUnexpected())
    await endpoint_client.broadcast(EventInherited())
    await endpoint_client.broadcast(EventTest())

    # enough cycles to allow the server to process the event
    await trio.sleep(0.05)

    assert len(results) == 2
    assert all(type(event) is EventTest for event in results)
