import pytest

from lahja.trio.endpoint import TrioEndpoint


@pytest.mark.skip
@pytest.mark.trio
async def test_trio_endpoint_server_start_and_stop(ipc_path, nursery):
    async with TrioEndpoint("test").run() as endpoint:
        server = EndpointServer(endpoint, ipc_path)

        assert server.is_running is False
        assert endpoint.is_running is True

        await server.start(nursery)

        assert server.is_running is True
        assert endpoint.is_running is True

        await server.stop()

        assert server.is_running is False
        assert endpoint.is_running is True


@pytest.mark.skip
@pytest.mark.trio
async def test_trio_endpoint_server_start_error_if_endpoint_not_running(
    ipc_path, nursery
):
    endpoint = TrioEndpoint("test")

    assert endpoint.is_running is False

    server = EndpointServer(endpoint, ipc_path)

    assert server.is_running is False

    with pytest.raises(Exception, match="TODO"):
        await server.start(nursery)

    assert server.is_running is False
    assert endpoint.is_running is False


@pytest.mark.skip
@pytest.mark.trio
async def test_trio_endpoint_server_stop_error_if_not_started(ipc_path):
    server = EndpointServer(TrioEndpoint("test"), ipc_path)

    with pytest.raises(Exception, match="TODO"):
        await server.stop()


@pytest.mark.skip
@pytest.mark.trio
async def test_trio_endpoint_server_stop_error_if_already_stopped(ipc_path, nursery):

    async with TrioEndpoint("test").run() as endpoint:
        server = EndpointServer(endpoint, ipc_path)

        await server.start(nursery)
        await server.stop()

        with pytest.raises(Exception, match="TODO"):
            await server.stop()
