import asyncio

import pytest

from lahja import BaseEvent


@pytest.mark.asyncio
async def test_local_broadcast_is_connected_to_self(endpoint):
    assert not endpoint.is_connected_to(endpoint.name)
    endpoint.enable_local_broadcast()
    assert endpoint.is_connected_to(endpoint.name)


@pytest.mark.asyncio
async def test_local_broadcast_wait_until_connected_to(endpoint):
    ready = asyncio.Event()

    async def do_enable():
        await ready.wait()
        endpoint.enable_local_broadcast()

    asyncio.ensure_future(do_enable())

    assert not endpoint.is_connected_to(endpoint.name)

    ready.set()
    await endpoint.wait_until_connected_to(endpoint.name)
    assert endpoint.is_connected_to(endpoint.name)


@pytest.mark.asyncio
async def test_local_broadcast_result_in_being_present_in_remotes(endpoint):
    before_names = set(
        name for name, _ in endpoint.get_connected_endpoints_and_subscriptions()
    )
    assert endpoint.name not in before_names

    endpoint.enable_local_broadcast()

    after_names = set(
        name for name, _ in endpoint.get_connected_endpoints_and_subscriptions()
    )
    assert endpoint.name in after_names


class Event(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_local_broadcast_wait_until_remote_subscriptions_change(endpoint):
    ready = asyncio.Event()
    done = asyncio.Event()

    async def do_wait():
        ready.set()
        await endpoint.wait_until_remote_subscriptions_change()
        done.set()

    asyncio.ensure_future(do_wait())

    await ready.wait()
    assert not done.is_set()
    endpoint.enable_local_broadcast()
    await done.wait()


@pytest.mark.asyncio
async def test_local_broadcast_wait_until_any_subscribed_to(endpoint):
    ready = asyncio.Event()

    async def do_enable():
        await ready.wait()
        endpoint.enable_local_broadcast()

    asyncio.ensure_future(do_enable())

    endpoint.subscribe(Event, lambda ev: None)

    assert not endpoint.is_any_remote_subscribed_to(Event)

    ready.set()
    await asyncio.wait_for(
        endpoint.wait_until_any_remote_subscribed_to(Event), timeout=0.01
    )
    assert endpoint.is_any_remote_subscribed_to(Event)


@pytest.mark.asyncio
async def test_local_broadcast_is_any_remote_subscribed(endpoint_pair):
    alice, bob = endpoint_pair

    assert not alice.is_any_remote_subscribed_to(Event)
    assert not bob.is_any_remote_subscribed_to(Event)

    alice.subscribe(Event, lambda ev: None)

    await bob.wait_until_remote_subscribed_to(alice.name, Event)

    assert not alice.is_any_remote_subscribed_to(Event)
    assert bob.is_any_remote_subscribed_to(Event)

    alice.enable_local_broadcast()

    assert alice.is_any_remote_subscribed_to(Event)
    assert bob.is_any_remote_subscribed_to(Event)


@pytest.mark.asyncio
async def test_local_broadcast_are_all_remotes_subscribed(endpoint_pair):
    alice, bob = endpoint_pair

    assert not alice.are_all_remotes_subscribed_to(Event)
    assert not bob.are_all_remotes_subscribed_to(Event)

    bob.subscribe(Event, lambda ev: None)

    await alice.wait_until_remote_subscribed_to(bob.name, Event)

    assert alice.are_all_remotes_subscribed_to(Event)
    assert not bob.are_all_remotes_subscribed_to(Event)

    alice.enable_local_broadcast()

    assert not alice.are_all_remotes_subscribed_to(Event)
    assert not bob.are_all_remotes_subscribed_to(Event)

    alice.subscribe(Event, lambda ev: None)
    await bob.wait_until_remote_subscribed_to(alice.name, Event)

    assert alice.are_all_remotes_subscribed_to(Event)
    assert bob.are_all_remotes_subscribed_to(Event)


@pytest.mark.asyncio
async def test_local_broadcast_is_remote_subscribed_to(endpoint):
    assert not endpoint.is_remote_subscribed_to(endpoint.name, Event)

    endpoint.subscribe(Event, lambda ev: None)

    assert not endpoint.is_remote_subscribed_to(endpoint.name, Event)

    endpoint.enable_local_broadcast()

    assert endpoint.is_remote_subscribed_to(endpoint.name, Event)


@pytest.mark.asyncio
async def test_local_broadcast_enables_local_broadcast(endpoint):
    did_handle = asyncio.Event()

    endpoint.subscribe(Event, lambda ev: did_handle.set())

    # broadcast a few
    await endpoint.broadcast(Event())
    await endpoint.broadcast(Event())
    await endpoint.broadcast(Event())
    assert not did_handle.is_set()

    endpoint.enable_local_broadcast()

    await endpoint.broadcast(Event())
    assert did_handle.is_set()
