import asyncio

import pytest

from lahja import BaseEvent, Endpoint


class StreamEvent(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_asyncio_stream_api_updates_subscriptions(endpoint_pair):
    subscriber, other = endpoint_pair
    remote = other._full_connections[subscriber.name]

    assert StreamEvent not in remote.subscribed_messages
    assert StreamEvent not in subscriber.subscribed_events

    stream_agen = subscriber.stream(StreamEvent, num_events=2)
    # start the generator in the background and give it a moment to start (so
    # that the subscription can get setup and propogated)
    fut = asyncio.ensure_future(stream_agen.asend(None))
    await asyncio.sleep(0.01)

    # broadcast the first event and grab and validate the first streamed
    # element.
    await other.broadcast(StreamEvent())
    event_1 = await fut
    assert isinstance(event_1, StreamEvent)

    # Now that we are within the stream, verify that the subscription is active
    # on the remote
    assert StreamEvent in remote.subscribed_messages
    assert StreamEvent in subscriber.subscribed_events

    # Broadcast and receive the second event, finishing the stream and
    # consequently the subscription
    await other.broadcast(StreamEvent())
    event_2 = await stream_agen.asend(None)
    assert isinstance(event_2, StreamEvent)
    await stream_agen.aclose()
    # give the subscription removal time to propagate.
    await asyncio.sleep(0.01)

    # Ensure the event is no longer in the subscriptions.
    assert StreamEvent not in remote.subscribed_messages
    assert StreamEvent not in subscriber.subscribed_events


@pytest.mark.asyncio
async def test_asyncio_wait_for_updates_subscriptions(endpoint_pair):
    subscriber, other = endpoint_pair
    remote = other._full_connections[subscriber.name]

    assert StreamEvent not in remote.subscribed_messages
    assert StreamEvent not in subscriber.subscribed_events

    # trigger a `wait_for` call to run in the background and give it a moment
    # to spin up.
    task = asyncio.ensure_future(subscriber.wait_for(StreamEvent))
    await asyncio.sleep(0.01)

    # Now that we are within the wait_for, verify that the subscription is active
    # on the remote
    assert StreamEvent in remote.subscribed_messages
    assert StreamEvent in subscriber.subscribed_events

    # Broadcast and receive the second event, finishing the stream and
    # consequently the subscription
    await other.broadcast(StreamEvent())
    event = await task
    assert isinstance(event, StreamEvent)
    # give the subscription removal time to propagate.
    await asyncio.sleep(0.01)

    # Ensure the event is no longer in the subscriptions.
    assert StreamEvent not in remote.subscribed_messages
    assert StreamEvent not in subscriber.subscribed_events


class InheretedStreamEvent(StreamEvent):
    pass


@pytest.mark.asyncio
async def test_asyncio_subscription_api_does_not_match_inherited_classes(endpoint_pair):
    subscriber, other = endpoint_pair
    remote = other._full_connections[subscriber.name]

    assert StreamEvent not in remote.subscribed_messages
    assert StreamEvent not in subscriber.subscribed_events

    # trigger a `wait_for` call to run in the background and give it a moment
    # to spin up.
    task = asyncio.ensure_future(subscriber.wait_for(StreamEvent))
    await asyncio.sleep(0.01)

    # Now that we are within the wait_for, verify that the subscription is active
    # on the remote
    assert StreamEvent in remote.subscribed_messages
    assert StreamEvent in subscriber.subscribed_events

    # Broadcast two of the inherited events and then the correct event.
    await other.broadcast(InheretedStreamEvent())
    await other.broadcast(InheretedStreamEvent())
    await other.broadcast(StreamEvent())

    # wait for a received event, finishing the stream and
    # consequently the subscription
    event = await task
    assert isinstance(event, StreamEvent)


class SubscribeEvent(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_asyncio_subscribe_updates_subscriptions(endpoint_pair):
    subscriber, other = endpoint_pair
    remote = other._full_connections[subscriber.name]

    assert SubscribeEvent not in remote.subscribed_messages
    assert SubscribeEvent not in subscriber.subscribed_events

    received_events = []

    # trigger a `wait_for` call to run in the background and give it a moment
    # to spin up.
    subscription = await subscriber.subscribe(SubscribeEvent, received_events.append)
    await asyncio.sleep(0.01)

    # Now that we are within the wait_for, verify that the subscription is active
    # on the remote
    assert SubscribeEvent in remote.subscribed_messages
    assert SubscribeEvent in subscriber.subscribed_events

    # Broadcast and receive the second event, finishing the stream and
    # consequently the subscription
    await other.broadcast(SubscribeEvent())
    # give time for propagation
    await asyncio.sleep(0.01)
    assert len(received_events) == 1
    event = received_events[0]
    assert isinstance(event, SubscribeEvent)

    # Ensure the event is still in the subscriptions.
    assert SubscribeEvent in remote.subscribed_messages
    assert SubscribeEvent in subscriber.subscribed_events

    subscription.unsubscribe()
    # give the subscription removal time to propagate.
    await asyncio.sleep(0.01)

    # Ensure the event is no longer in the subscriptions.
    assert SubscribeEvent not in remote.subscribed_messages
    assert SubscribeEvent not in subscriber.subscribed_events


@pytest.fixture
async def server_with_three_connections(ipc_base_path, server_config, endpoint_server):
    async with Endpoint("client-a").run() as client_a:
        async with Endpoint("client-b").run() as client_b:
            async with Endpoint("client-c").run() as client_c:
                await client_a.connect_to_endpoint(server_config)
                await client_b.connect_to_endpoint(server_config)
                await client_c.connect_to_endpoint(server_config)

                await endpoint_server.wait_connected_to("client-a")
                await endpoint_server.wait_connected_to("client-b")
                await endpoint_server.wait_connected_to("client-c")
                yield endpoint_server, client_a, client_b, client_c


class WaitSubscription(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_asyncio_wait_until_any_connection_subscribed_to(
    server_with_three_connections
):
    server, client_a, client_b, client_c = server_with_three_connections

    got_subscription = asyncio.Event()

    async def do_wait_subscriptions():
        await server.wait_until_any_connection_subscribed_to(WaitSubscription)
        got_subscription.set()

    asyncio.ensure_future(do_wait_subscriptions())

    await client_a.subscribe(WaitSubscription, id)
    assert got_subscription.is_set() is True


@pytest.mark.asyncio
async def test_asyncio_wait_until_all_connection_subscribed_to(
    server_with_three_connections
):
    server, client_a, client_b, client_c = server_with_three_connections

    got_subscription = asyncio.Event()

    async def do_wait_subscriptions():
        await server.wait_until_all_connection_subscribed_to(WaitSubscription)
        got_subscription.set()

    asyncio.ensure_future(do_wait_subscriptions())

    assert len(server._full_connections) + len(server._half_connections) == 3

    await client_c.subscribe(WaitSubscription, id)
    assert got_subscription.is_set() is False
    await client_a.subscribe(WaitSubscription, id)
    assert got_subscription.is_set() is False
    await client_b.subscribe(WaitSubscription, id)
    await asyncio.sleep(0.01)
    assert got_subscription.is_set() is True
