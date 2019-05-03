import pytest

import curio

from lahja import (
    BaseEvent,
)


class EventTest(BaseEvent):
    def __init__(self, value):
        self.value = value


@pytest.mark.curio
async def test_curio_endpoint_wait_for(endpoint_pair):
    alice, bob = endpoint_pair

    # NOTE: this test is the inverse of the broadcast test
    event = EventTest('test')

    async def _do_wait_for():
        result = await alice.wait_for(EventTest)
        return result

    task = await curio.spawn(_do_wait_for)

    await bob.broadcast(event)

    result = await task.join()
    assert isinstance(result, EventTest)
    assert result.value == 'test'
