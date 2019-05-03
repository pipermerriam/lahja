import pytest

import curio

from lahja import (
    BaseEvent,
)


class EventTest(BaseEvent):
    def __init__(self, value):
        self.value = value


@pytest.mark.curio
async def test_curio_endpoint_stream_without_limit(endpoint_pair):
    alice, bob = endpoint_pair

    async def _do_stream():
        results = []
        async for event in alice.stream(EventTest):
            results.append(event)
            if len(results) == 4:
                break
        return results

    task = await curio.spawn(_do_stream)

    await bob.broadcast(EventTest(0))
    await bob.broadcast(EventTest(1))
    await bob.broadcast(EventTest(2))
    await bob.broadcast(EventTest(3))

    results = await task.join()
    assert len(results) == 4
    assert all(isinstance(result, EventTest) for result in results)

    values = [result.value for result in results]
    assert values == [0, 1, 2, 3]


@pytest.mark.curio
async def test_curio_endpoint_stream_with_limit(endpoint_pair):
    alice, bob = endpoint_pair

    async def _do_stream():
        results = []
        async for event in alice.stream(EventTest, num_events=4):
            results.append(event)
            assert len(results) <= 4
        return results

    task = await curio.spawn(_do_stream)

    await bob.broadcast(EventTest(0))
    await bob.broadcast(EventTest(1))
    await bob.broadcast(EventTest(2))
    await bob.broadcast(EventTest(3))

    results = await task.join()
    assert len(results) == 4
    assert all(isinstance(result, EventTest) for result in results)

    values = [result.value for result in results]
    assert values == [0, 1, 2, 3]
