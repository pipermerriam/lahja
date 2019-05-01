import collections
import functools
import itertools
import logging
import pathlib
import pickle
from types import (
    TracebackType,
)
from typing import (
    AsyncGenerator,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)
import weakref

import curio

from lahja.endpoint import (
    BaseEndpoint,
    Broadcast,
    ConnectionConfig,
)
from lahja.exceptions import (
    ConnectionAttemptRejected,
)
from lahja.misc import (
    TRANSPARENT_EVENT,
    BaseEvent,
    BaseRequestResponseEvent,
    BroadcastConfig,
    Subscription,
)


class RemoteEndpoint:
    logger = logging.getLogger('lahja.curio.RemoteEndpoint')

    def __init__(self, socket: curio.io.Socket) -> None:
        self._socket = socket

    @classmethod
    async def connect_to(cls, config: ConnectionConfig) -> 'RemoteEndpoint':
        socket = await curio.open_unix_connection(str(config.path))
        cls.logger.info('Opened connection to %s[%s]: %s', config.name, config.path, socket)
        return cls(socket)

    async def send_message(self, message: Broadcast) -> None:
        assert isinstance(message, Broadcast)
        msg_data = pickle.dumps(message)
        size = len(msg_data)
        await self._socket.sendall(size.to_bytes(4, 'little'))
        await self._socket.sendall(msg_data)

    async def read_message(self) -> Broadcast:
        msg_size_data = await self._socket.recv(4)
        assert len(msg_size_data) == 4
        msg_size = int.from_bytes(msg_size_data, 'little')
        msg_data = await self._socket.recv(msg_size)
        message = pickle.loads(msg_data)
        assert isinstance(message, Broadcast)  # maybe this should be a user-friendly error?
        return message


class CurioEndpoint(BaseEndpoint):
    _name: str = None
    _ipc_path: pathlib.Path = None

    _server_running: curio.Event
    _server_stopped: curio.Event
    _server_task: curio.Task

    _connected_endpoints: Dict[str, RemoteEndpoint]

    _internal_queue: curio.Queue
    _external_queue: curio.Queue
    _stream_queues: Dict[Type[BaseEvent], List[curio.Queue]]

    _has_snappy_support: bool = None

    logger = logging.getLogger('lahja.curio.CurioEndpoint')

    def __init__(self):
        self._server_lock = curio.Lock()
        self._server_running = curio.Event()
        self._server_stopped = curio.Event()

        # Temporary storage for queues used in the `stream` API.  Uses a
        # `WeakSet` to automate cleanup of queues that are no longer needed.
        self._stream_queues = collections.defaultdict(weakref.WeakSet)

        # This lock ensures that the server doesn't close it's task group while
        # a connection attempt is being handled.
        self._shutdown_lock = curio.Lock()
        self._connected_endpoints = {}

    @property
    def is_serving(self):
        """
        locked, running and not stopped
        """
        if not self._server_lock.locked():
            return False
        elif not self._server_running.is_set():
            return False
        elif self._server_stopped.is_set():
            return False
        else:
            return True

    @property
    def ipc_path(self) -> pathlib.Path:
        return self._ipc_path

    @property
    def name(self) -> str:
        return self._name

    @property
    def has_snappy_support(self) -> bool:
        if self._has_snappy_support is None:
            from lahja.snappy import is_snappy_available
            self._has_snappy_support = is_snappy_available
        return self._has_snappy_support

    def start_serving_nowait(self, connection_config: ConnectionConfig)-> None:
        """
        Start serving this :class:`~lahja.endpoint.Endpoint` so that it can receive events. It is
        not guaranteed that the :class:`~lahja.endpoint.Endpoint` is fully ready after this method
        returns. Use :meth:`~lahja.endpoint.Endpoint.start_serving` or combine with
        :meth:`~lahja.endpoint.Endpoint.wait_until_serving`
        """
        assert False

    async def start_serving(self, connection_config: ConnectionConfig) -> None:
        """
        Start serving this :class:`~lahja.endpoint.Endpoint` so that it can receive events. Await
        until the :class:`~lahja.endpoint.Endpoint` is ready.
        """
        self._name = connection_config.name
        self._ipc_path = connection_config.path

        self._internal_loop_running = curio.Event()
        self._external_loop_running = curio.Event()

        self._server_task = await curio.spawn(self._run_server, daemon=True)

        async with curio.TaskGroup() as group:
            await group.spawn(self._server_running.wait)
            await group.spawn(self._internal_loop_running.wait)
            await group.spawn(self._external_loop_running.wait)

    async def wait_until_serving(self) -> None:
        """
        Await until the ``Endpoint`` is ready to receive events.
        """
        assert False

    async def _clean_finished_tasks(self, group: curio.TaskGroup) -> None:
        async for task in group:
            self.logger.debug("Cleaned taske: %s", task)

    async def _run_server(self) -> None:
        # TODO: rather than having all of this temporary state that only exists
        # for the lifecycle of the server, lets isolate the server components
        # into their own class and give them appropriate handles to the stream
        # queues.
        async with self._server_lock:
            self._internal_queue = curio.Queue()
            self._external_queue = curio.Queue()

            await self._server_running.set()
            async with curio.TaskGroup() as group:
                await group.spawn(self._clean_finished_tasks, group)
                await group.spawn(self._connect_internal_queue, group)
                await group.spawn(self._connect_external_queue, group)

                self.logger.debug("Endpoint starting: %s", self.name)
                try:
                    await curio.unix_server(
                        str(self.ipc_path),
                        functools.partial(self._accept_connection, group),
                    )
                except curio.TaskCancelled:
                    pass
                finally:
                    self.logger.debug("Endpoint stopping: %s", self.name)
                    async with self._shutdown_lock:
                        await group.cancel_remaining()
                        await self._server_stopped.set()

            del self._internal_queue
            del self._external_queue
            del self._internal_loop_running
            del self._external_loop_running
            del self._name
            del self._ipc_path
            self.logger.debug("Endpoint stopped: %s", self.name)

    async def _accept_connection(self,
                                 group: curio.TaskGroup,
                                 socket: curio.io.Socket,
                                 address: str) -> None:
        self.logger.info('accepting connection: %s @ %s', socket, address)
        remote = RemoteEndpoint(socket)

        # This lock ensures that the `group` doesn't close while we are still
        # trying to start the handler.
        async with self._shutdown_lock:
            if self._server_stopped.is_set():
                self.logger.debug("Server shutting down.  Aborting client connection: %s", remote)
                return
            else:
                task = await group.spawn(self._handle_client, remote)

        await task.join()

    async def _handle_client(self, remote: RemoteEndpoint) -> None:
        self.logger.info('starting client handler for: %s', remote)
        await curio.sleep(0.1)
        while self.is_serving:
            message = await remote.read_message()

            if isinstance(message, Broadcast):
                await self._external_queue.put((message.event, message.config))
            else:
                self.logger.warning(
                    f'[{self.ipc_path}] received unexpected message: {message}'
                )

    #
    # Compression
    #
    def _compress_event(self, event: BaseEvent) -> Union[BaseEvent, bytes]:
        if self.has_snappy_support:
            import snappy
            return cast(bytes, snappy.compress(pickle.dumps(event)))
        else:
            return event

    def _decompress_event(self, data: Union[BaseEvent, bytes]) -> BaseEvent:
        if isinstance(data, BaseEvent):
            return data
        else:
            import snappy
            return cast(BaseEvent, pickle.loads(snappy.decompress(data)))

    def _throw_if_already_connected(self, *endpoints: ConnectionConfig) -> None:
        assert False

    async def _connect_external_queue(self, group) -> None:
        await self._external_loop_running.set()
        while self.is_serving:
            (item, config) = await self._external_queue.get()
            event = self._decompress_event(item)
            await group.spawn(self._process_item, event, config)

    async def _connect_internal_queue(self, group) -> None:
        await self._internal_loop_running.set()
        while self.is_serving:
            (item, config) = await self._internal_queue.get()
            await group.spawn(self._process_item, item, config)

    async def connect_to_endpoints(self, *endpoints: ConnectionConfig) -> None:
        """
        Connect to the given endpoints and await until all connections are established.
        """
        assert False

    def connect_to_endpoints_nowait(self, *endpoints: ConnectionConfig) -> None:
        """
        Connect to the given endpoints as soon as they become available but do not block.
        """
        assert False

    async def _await_connect_to_endpoint(self, endpoint: ConnectionConfig) -> None:
        assert False

    async def connect_to_endpoint(self, config: ConnectionConfig) -> None:
        if config.name in self._connected_endpoints:
            raise ConnectionAttemptRejected(
                f"Already connected to endpoint with name: {config.name}"
            )
        endpoint = await RemoteEndpoint.connect_to(config)
        self._connected_endpoints[config.name] = endpoint

    def is_connected_to(self, endpoint_name: str) -> bool:
        return endpoint_name in self._connected_endpoints

    async def _process_item(self, item: BaseEvent, config: BroadcastConfig) -> None:
        if item is TRANSPARENT_EVENT:
            return

        event_type = type(item)

        in_queue = event_type in self._stream_queues

        if not in_queue:
            return

        if in_queue:
            for queue in self._stream_queues[event_type]:
                await queue.put(item)

    async def stop(self) -> None:
        """
        Stop the :class:`~lahja.endpoint.Endpoint` from receiving further events.
        """
        self.logger.info('Endpoint.stop()')
        if not self.is_serving:
            return
        await self._server_task.cancel()

    def stop_nowait(self) -> None:
        assert False

    def __enter__(self) -> 'Endpoint':
        return self

    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 exc_tb: Optional[TracebackType]) -> None:
        self.stop()

    async def broadcast(self, item: BaseEvent, config: Optional[BroadcastConfig] = None) -> None:
        """
        Broadcast an instance of :class:`~lahja.misc.BaseEvent` on the event bus. Takes
        an optional second parameter of :class:`~lahja.misc.BroadcastConfig` to decide
        where this event should be broadcasted to. By default, events are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        self.logger.debug('Broadcasting %s -> %s', item, config)
        item._origin = self.name

        if config is not None and config.internal:
            self.logger.info('A')
            # Internal events simply bypass going through the central event bus
            # and are directly put into the local receiving queue instead.
            await self._internal_queue.put((item, config))
        else:
            # Broadcast to every connected Endpoint that is allowed to receive the event
            remotes_for_broadcast = self._allowed_remotes_for_config(config)
            if not remotes_for_broadcast:
                self.logger.warning(
                    "Broadcast of %s with config %s matched 0/%d connected endpoints",
                    item,
                    config,
                    len(self._connected_endpoints),
                )
            else:
                compressed_item = self._compress_event(item)
                async with curio.TaskGroup() as group:
                    for remote in remotes_for_broadcast:
                        await group.spawn(remote.send_message, Broadcast(compressed_item, config))

    def _allowed_remotes_for_config(self,
                                    config: Optional[BroadcastConfig],
                                    ) -> Tuple[RemoteEndpoint, ...]:
        if config is None:
            return tuple(self._connected_endpoints.values())

        return tuple(
            remote
            for name, remote in self._connected_endpoints.items()
            if config.allowed_to_receive(name)
        )

    def broadcast_nowait(self,
                         item: BaseEvent,
                         config: Optional[BroadcastConfig] = None) -> None:
        """
        A non-async `broadcast()` (see the docstring for `broadcast()` for more)

        Instead of blocking the calling coroutine this function schedules the broadcast
        and immediately returns.

        CAUTION: You probably don't want to use this. broadcast() doesn't return until the
        write socket has finished draining, meaning that the OS has accepted the message.
        This prevents us from sending more data than the remote process can handle.
        broadcast_nowait has no such backpressure. Even after the remote process stops
        accepting new messages this function will continue to accept them, which in the
        worst case could lead to runaway memory usage.
        """
        assert False

    TResponse = TypeVar('TResponse', bound=BaseEvent)

    async def request(self,
                      item: BaseRequestResponseEvent[TResponse],
                      config: Optional[BroadcastConfig] = None) -> TResponse:
        """
        Broadcast an instance of :class:`~lahja.misc.BaseRequestResponseEvent` on the event bus and
        immediately wait on an expected answer of type :class:`~lahja.misc.BaseEvent`. Optionally
        pass a second parameter of :class:`~lahja.misc.BroadcastConfig` to decide where the request
        should be broadcasted to. By default, requests are broadcasted across all connected
        endpoints with their consuming call sites.
        """
        assert False

    TSubscribeEvent = TypeVar('TSubscribeEvent', bound=BaseEvent)

    def subscribe(self,
                  event_type: Type[TSubscribeEvent],
                  handler: Callable[[TSubscribeEvent], None]) -> Subscription:
        """
        Subscribe to receive updates for any event that matches the specified event type.
        A handler is passed as a second argument an :class:`~lahja.misc.Subscription` is returned
        to unsubscribe from the event if needed.
        """
        assert False

    TStreamEvent = TypeVar('TStreamEvent', bound=BaseEvent)

    async def stream(self,
                     event_type: Type[TStreamEvent],
                     num_events: Optional[int] = None) -> AsyncGenerator[TStreamEvent, None]:
        """
        Stream all events that match the specified event type. This returns an
        ``AsyncIterable[BaseEvent]`` which can be consumed through an ``async for`` loop.
        An optional ``num_events`` parameter can be passed to stop streaming after a maximum amount
        of events was received.
        """
        queue = curio.Queue()

        self._stream_queues[event_type].add(queue)

        if num_events is None:
            # iterate forever
            counter = itertools.count()
        else:
            # fixed number of iterations
            counter = iter(range(num_events))

        while True:
            try:
                yield await queue.get()
            except GeneratorExit:
                break
            except Exception as err:
                self.logger.debug('Unhandled exception during event streaming: %s', err)
                raise
            else:
                try:
                    next(counter)
                except StopIteration:
                    break

        # Because we use a `weakref.WeakSet` for storage the queue is
        # automatically removed when this function exits.

    TWaitForEvent = TypeVar('TWaitForEvent', bound=BaseEvent)

    async def wait_for(self, event_type: Type[TWaitForEvent]) -> TWaitForEvent:
        """
        Wait for a single instance of an event that matches the specified event type.
        """
        async for event in self.stream(event_type, num_events=1):
            return event
