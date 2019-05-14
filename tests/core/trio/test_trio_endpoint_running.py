import pytest

from lahja.trio.endpoint import TrioEndpoint


@pytest.mark.trio
async def test_trio_endpoint_start_and_stop(nursery):
    endpoint = TrioEndpoint("test")
    assert endpoint.is_running is False
    await endpoint.start(nursery)
    assert endpoint.is_running is True
    await endpoint.stop()
    assert endpoint.is_running is False


@pytest.mark.trio
async def test_trio_endpoint_as_contextmanager():
    endpoint = TrioEndpoint("test")
    assert endpoint.is_running is False

    async with endpoint.run():
        assert endpoint.is_running is True
    assert endpoint.is_running is False


@pytest.mark.trio
async def test_trio_endpoint_as_contextmanager_inline():
    async with TrioEndpoint("test").run() as endpoint:
        assert endpoint.is_running is True
    assert endpoint.is_running is False


@pytest.mark.trio
async def test_trio_endpoint_stop_error_if_not_started():
    endpoint = TrioEndpoint("test")
    with pytest.raises(Exception, match="TODO"):
        await endpoint.stop()


@pytest.mark.trio
async def test_trio_endpoint_stop_noop_if_already_stopped():
    endpoint = TrioEndpoint("test")
    async with endpoint.run():
        pass

    assert not endpoint.is_running
    assert endpoint.is_stopped

    await endpoint.stop()
