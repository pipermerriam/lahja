import pytest

from lahja.curio.endpoint import (
    CurioEndpoint,
    EndpointServer,
)


@pytest.mark.curio
async def test_curio_endpoint_server_start_and_stop(ipc_path):
    endpoint = CurioEndpoint('test')
    server = EndpointServer(endpoint, ipc_path)

    assert server.is_running is False
    assert endpoint.is_running is False

    await server.start()

    assert server.is_running is True
    assert endpoint.is_running is True

    await server.stop()

    assert server.is_running is False
    assert endpoint.is_running is False


@pytest.mark.curio
async def test_curio_endpoint_server_as_contextmanager(ipc_path):
    endpoint = CurioEndpoint('test')
    server = EndpointServer(endpoint, ipc_path)

    assert server.is_running is False
    assert endpoint.is_running is False

    async with server:
        assert server.is_running is True
        assert endpoint.is_running is True

    assert server.is_running is False
    assert endpoint.is_running is False


@pytest.mark.curio
async def test_curio_endpoint_server_handles_already_started_endpoint(ipc_path):
    async with CurioEndpoint('test') as endpoint:
        assert endpoint.is_running is True

        server = EndpointServer(endpoint, ipc_path)

        assert server.is_running is False

        async with server:
            assert server.is_running is True
            assert endpoint.is_running is True

        assert server.is_running is False
        # should still be running since server wasn't responsible for starting
        assert endpoint.is_running is True

    assert endpoint.is_running is False


@pytest.mark.curio
async def test_curio_endpoint_server_stop_error_if_not_started(ipc_path):
    server = EndpointServer(CurioEndpoint('test'), ipc_path)

    with pytest.raises(Exception, match='TODO'):
        await server.stop()


@pytest.mark.curio
async def test_curio_endpoint_server_stop_error_if_already_stopped(ipc_path):
    server = EndpointServer(CurioEndpoint('test'), ipc_path)

    async with server:
        pass

    with pytest.raises(Exception, match='TODO'):
        await server.stop()
