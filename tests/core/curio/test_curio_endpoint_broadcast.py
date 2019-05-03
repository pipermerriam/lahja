import pytest

import curio

from lahja import (
    BaseEvent,
)


class EventTest(BaseEvent):
    def __init__(self, value):
        self.value = value


@pytest.mark.curio
async def test_curio_endpoint_broadcast(endpoint_pair):
    alice, bob = endpoint_pair

    event = EventTest('test')

    task = await curio.spawn(alice.wait_for, EventTest)

    await bob.broadcast(event)

    result = await task.join()
    assert isinstance(result, EventTest)
    assert result.value == 'test'
