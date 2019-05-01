import functools
import inspect
from pathlib import Path
import tempfile
import uuid
import logging

import curio

from async_generator import isasyncgenfunction

import pytest

from lahja import (
    ConnectionConfig,
)
from lahja.curio.endpoint import (
    CurioEndpoint,
)


def generate_unique_name() -> str:
    # We use unique names to avoid clashing of IPC pipes
    return str(uuid.uuid4())


CURIO_MARK_NAME = 'curio'
KERNEL_FIXTURE_NAME = 'kernel'
REQUEST_FIXTURE_NAME = 'request'


def pytest_configure(config):
    config.addinivalue_line("markers",
                            "curio: "
                            "mark the test as a coroutine, it will be "
                            "run using a curio kernel")


@pytest.mark.tryfirst
def pytest_pycollect_makeitem(collector, name, obj):
    if collector.funcnamefilter(name) and inspect.iscoroutinefunction(obj):
        item = pytest.Function(name, parent=collector)
        if CURIO_MARK_NAME in item.keywords:
            return list(collector._genfunctions(name, obj))


@pytest.hookimpl(hookwrapper=True)
def pytest_fixture_setup(fixturedef, request):
    """
    Ensure that async fixtures work in a curio context.
    """
    if isasyncgenfunction(fixturedef.func):
        # This is an async generator function. Wrap it accordingly.
        f = fixturedef.func

        if KERNEL_FIXTURE_NAME not in fixturedef.argnames:
            fixturedef.argnames += (KERNEL_FIXTURE_NAME, )
            strip_kernel = True
        else:
            strip_kernel = False

        if REQUEST_FIXTURE_NAME not in fixturedef.argnames:
            fixturedef.argnames += (REQUEST_FIXTURE_NAME, )
            strip_request = True
        else:
            strip_request = False

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            kernel = kwargs[KERNEL_FIXTURE_NAME]
            request = kwargs[REQUEST_FIXTURE_NAME]
            if strip_kernel:
                del kwargs[KERNEL_FIXTURE_NAME]
            if strip_request:
                del kwargs[REQUEST_FIXTURE_NAME]

            gen_obj = curio.meta.finalize(f(*args, **kwargs))
            agen = None

            async def setup():
                logging.info('FIXTURE STARTING')
                nonlocal agen
                agen = await gen_obj.__aenter__()
                res = await agen.__anext__()
                return res

            def finalizer():
                """Yield again, to finalize."""
                async def async_finalizer():
                    logging.info('FINALIZER STOPPING')
                    try:
                        await agen.__anext__()
                    except StopAsyncIteration:
                        logging.info('FINALIZER NATURAL STOP')
                        pass
                    else:
                        raise ValueError(f"Async generator fixture {f} didn't stop")
                    finally:
                        await gen_obj.__aexit__(None, None, None)
                kernel.run(async_finalizer)

            request.addfinalizer(finalizer)

            return kernel.run(setup)

        fixturedef.func = wrapper

    elif inspect.iscoroutinefunction(fixturedef.func):
        # Just a coroutine, not an async generator.
        f = fixturedef.func

        if KERNEL_FIXTURE_NAME not in fixturedef.argnames:
            fixturedef.argnames += (KERNEL_FIXTURE_NAME, )
            strip_kernel = True
        else:
            strip_kernel = False

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            kernel = kwargs[KERNEL_FIXTURE_NAME]
            if strip_kernel:
                del kwargs[KERNEL_FIXTURE_NAME]

            async def setup():
                res = await f(*args, **kwargs)
                return res

            return kernel.run(setup)

        fixturedef.func = wrapper
    else:
        pass

    yield


@pytest.mark.tryfirst
def pytest_pyfunc_call(pyfuncitem):
    """
    Run curio marked test functions in a kernel instead of a normal
    function call.
    """
    if CURIO_MARK_NAME in pyfuncitem.keywords:
        if not inspect.iscoroutinefunction(pyfuncitem.obj):
            raise TypeError("The `curio` mark is only valid on coroutines")
        kernel = pyfuncitem.funcargs[KERNEL_FIXTURE_NAME]
        testargs = {
            arg: pyfuncitem.funcargs[arg]
            for arg in pyfuncitem._fixtureinfo.argnames
        }
        fut = functools.partial(pyfuncitem.obj, **testargs)
        kernel.run(fut)
        return True


def pytest_runtest_setup(item):
    if CURIO_MARK_NAME in item.keywords and KERNEL_FIXTURE_NAME not in item.fixturenames:
        # inject a kernel fixture for all async tests with the `curio` mark
        item.fixturenames.append(KERNEL_FIXTURE_NAME)


@pytest.fixture
def kernel():
    """Create an instance of the default kernel for each test case."""
    with curio.Kernel() as kernel:
        yield kernel


@pytest.fixture
def ipc_base_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
async def endpoint(ipc_base_path):
    endpoint = CurioEndpoint()
    await endpoint.start_serving(
        ConnectionConfig.from_name(generate_unique_name(), base_path=ipc_base_path)
    )
    # We need to connect to our own Endpoint if we care about receiving
    # the events we broadcast. Many tests use the same Endpoint for
    # broadcasting and receiving which is a valid use case so we hook it up
    await endpoint.connect_to_endpoint(
        ConnectionConfig.from_name(endpoint.name, base_path=ipc_base_path),
    )
    try:
        yield endpoint
    finally:
        await endpoint.stop()
