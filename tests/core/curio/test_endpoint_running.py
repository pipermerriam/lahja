import pytest

from lahja.curio.endpoint import (
    CurioEndpoint,
)


@pytest.mark.curio
async def test_curio_endpoint_start_and_stop():
    endpoint = CurioEndpoint('test')
    assert endpoint.is_running is False
    await endpoint.start()
    assert endpoint.is_running is True
    await endpoint.stop()
    assert endpoint.is_running is False


@pytest.mark.curio
async def test_curio_endpoint_as_contextmanager():
    endpoint = CurioEndpoint('test')
    assert endpoint.is_running is False

    async with endpoint:
        assert endpoint.is_running is True
    assert endpoint.is_running is False


@pytest.mark.curio
async def test_curio_endpoint_as_contextmanager_inline():
    async with CurioEndpoint('test') as endpoint:
        assert endpoint.is_running is True
    assert endpoint.is_running is False


@pytest.mark.curio
async def test_curio_endpoint_stop_error_if_not_started():
    endpoint = CurioEndpoint('test')
    with pytest.raises(Exception, match='TODO'):
        await endpoint.stop()


@pytest.mark.curio
async def test_curio_endpoint_stop_error_if_already_stopped():
    endpoint = CurioEndpoint('test')
    async with endpoint:
        pass

    assert not endpoint.is_running

    with pytest.raises(Exception, match='TODO'):
        await endpoint.stop()
