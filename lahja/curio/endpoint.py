import collections
import functools
import itertools
import logging
from pathlib import Path
import pickle
from types import (
    TracebackType,
)
from typing import (
    Any,
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
import uuid
import weakref

import curio

from lahja.endpoint import (
    Broadcast,
    ConnectionConfig,
)
from lahja.exceptions import (
    ConnectionAttemptRejected,
    UnexpectedResponse,
)
from lahja.misc import (
    _Hello,
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
        self._stream = self._socket.as_stream()

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
        msg_size_data = await self._stream.read_exactly(4)
        if len(msg_size_data) != 4:
            raise Exception(f"msg size was: {len(msg_size_data)}")
        assert len(msg_size_data) == 4
        msg_size = int.from_bytes(msg_size_data, 'little')
        msg_data = await self._stream.read_exactly(msg_size)
        message = pickle.loads(msg_data)
        assert isinstance(message, Broadcast)  # maybe this should be a user-friendly error?
        return message


async def _clean_finished_tasks(group: curio.TaskGroup) -> None:
    async for task in group:  # noqa: F841
        pass


class CurioEndpoint:
    _ipc_path: Path = None

    _connected_endpoints: Dict[str, RemoteEndpoint]

    _stream_queues: Dict[Type[BaseEvent], List[curio.Queue]]
    _pending_requests: Dict[str, AsyncGenerator]
    _handlers: Dict[Type[BaseEvent], List[Callable[[BaseEvent], Any]]]

    _has_snappy_support: bool = None

    logger = logging.getLogger('lahja.curio.CurioEndpoint')

    def __init__(self, name: str):
        self.name = name

        # Temporary storage for queues used in the `stream` API.  Uses a
        # `WeakSet` to automate cleanup of queues that are no longer needed.
        self._stream_queues = collections.defaultdict(weakref.WeakSet)
        self._pending_requests = {}
        self._handlers = collections.defaultdict(list)

        # This lock ensures that the server doesn't close it's task group while
        # a connection attempt is being handled.
        self._connected_endpoints = {}

        self._run_lock = curio.Lock()

        self._running = curio.Event()
        self._finished = curio.Event()

    def __str__(self) -> str:
        return f"Endpoint[{self.name}]"

    def __repr__(self) -> str:
        return f"<{self}>"

    @property
    def is_running(self):
        """
        locked, running and not stopped
        """
        if not self._run_lock.locked():
            return False
        elif not self._running.is_set():
            return False
        elif self._finished.is_set():
            return False
        else:
            return True

    @property
    def has_snappy_support(self) -> bool:
        if self._has_snappy_support is None:
            from lahja.snappy import is_snappy_available
            self._has_snappy_support = is_snappy_available
        return self._has_snappy_support

    #
    # Compression
    #
    def _compress_event(self, event: BaseEvent) -> Union[BaseEvent, bytes]:
        if self.has_snappy_support:
            import snappy
            return cast(bytes, snappy.compress(pickle.dumps(event)))
        else:
            return event

    #
    # Establishing connections
    #
    def direct_connect(self, name: str, remote: RemoteEndpoint) -> None:
        self.logger.debug(
            "Endpoint %s direct connect to %s:%s",
            self.name,
            name,
            remote,
        )
        if name in self._connected_endpoints:
            raise ConnectionAttemptRejected(
                f"Already connected to endpoint with name: {name}"
            )
        self._connected_endpoints[name] = remote

    async def connect_to_endpoints(self, *endpoints: ConnectionConfig) -> None:
        """
        Connect to the given endpoints and await until all connections are established.
        """
        # TODO: need tests
        async with curio.TaskGroup() as group:
            for config in endpoints:
                await group.spawn(self.connect_to_endpoint, config)

    async def connect_to_endpoint(self, config: ConnectionConfig) -> None:
        if not self.is_running:
            raise Exception("TODO: cannot establish connections if endpoint isn't running")

        if config.name in self._connected_endpoints:
            raise ConnectionAttemptRejected(
                f"Already connected to endpoint with name: {config.name}"
            )
        remote = await RemoteEndpoint.connect_to(config)
        self._connected_endpoints[config.name] = remote
        # send the hello message to allow a reverse connection to be established.
        await self.broadcast(_Hello(), BroadcastConfig(config.name))
        # ensure we also read incoming messages from the other side of this connection
        await self._reverse_connect_queue.put(remote)

    def is_connected_to(self, endpoint_name: str) -> bool:
        return endpoint_name in self._connected_endpoints

    #
    # Internal event processing logic
    #
    async def _process_item(self, item: BaseEvent, config: BroadcastConfig) -> None:
        self.logger.debug(
            "Endpoint %s processing item: %s  with config: %s",
            self.name,
            item,
            config,
        )
        if item is TRANSPARENT_EVENT:
            # TODO: why?
            return

        event_type = type(item)
        has_config = config is not None

        # handle request/response
        in_pending_requests = has_config and config.filter_event_id in self._pending_requests

        if in_pending_requests:
            agen = self._pending_requests.pop(config.filter_event_id)
            await agen.asend(item)

        # handle stream queue
        in_queue = event_type in self._stream_queues

        if in_queue:
            for queue in self._stream_queues[event_type]:
                await queue.put(item)

        # handle subscriptions
        in_handlers = event_type in self._handlers

        if in_handlers:
            for handler_fn in self._handlers[event_type]:
                try:
                    handler_fn(item)
                except Exception as err:
                    self.logger.debug(
                        "Endpoint %s handler function %s error: %s",
                        self.name,
                        handler_fn,
                        err,
                    )

    #
    # Running and endpoint
    #
    async def start(self) -> None:
        self._internal_loop_running = curio.Event()
        self._reverse_connect_loop_running = curio.Event()
        self._shutdown_trigger = curio.Event()

        self._run_task = await curio.spawn(self._run_endpoint)

        async with curio.TaskGroup() as group:
            await group.spawn(self._running.wait)
            await group.spawn(self._internal_loop_running.wait)
            await group.spawn(self._reverse_connect_loop_running.wait)
        self.logger.debug("Endpoint %s started", self.name)

    async def stop(self) -> None:
        """
        Stop the :class:`~lahja.endpoint.Endpoint` from receiving further events.
        """
        if not self._running.is_set():
            raise Exception("TODO: never run")
        if self._finished.is_set():
            raise Exception("TODO: already stopped")

        self.logger.debug('in ENDPOINT stop')
        await self._run_task.cancel()
        self.logger.debug('ENDPOINT cancelled')

        self.logger.debug("Endpoint %s stopped", self.name)

    async def __aenter__(self) -> 'Endpoint':
        # TODO: this should start the *internal* queue part and needs to be
        # linked up to the server's internal queue somehow
        await self.start()
        return self

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_value: Optional[BaseException],
                        exc_tb: Optional[TracebackType]) -> None:
        await self.stop()

    async def _run_endpoint(self) -> None:
        async with self._run_lock:
            self._message_queue = curio.Queue()
            self._reverse_connect_queue = curio.Queue()

            self.logger.debug("Endpoint %s starting", self.name)
            await self._running.set()

            async with curio.TaskGroup() as group:
                await group.spawn(_clean_finished_tasks, group)
                await group.spawn(self._connect_message_queue, group)
                await group.spawn(self._connect_reverse_connect_queue, group)

                try:
                    await self._shutdown_trigger.wait()
                except curio.TaskCancelled:
                    self.logger.info(
                        'Endpoind %s received cancellation',
                        self.name,
                    )
                    pass
                finally:
                    self.logger.debug("Endpoint %s stopping", self.name)
                    await group.cancel_remaining()

            del self._message_queue
            del self._reverse_connect_queue
            del self._internal_loop_running

            await self._finished.set()

    async def _connect_reverse_connect_queue(self, group: curio.TaskGroup) -> None:
        self.logger.debug("Endpoint %s starting reverse connect queue", self.name)
        await self._reverse_connect_loop_running.set()
        while self.is_running:
            remote = await self._reverse_connect_queue.get()
            await group.spawn(self._handle_reverse_connect, remote)

    async def _handle_reverse_connect(self, remote: RemoteEndpoint) -> None:
        self.logger.info(
            'Endpoint %s starting client handler for %s',
            self.name,
            remote
        )
        while self.is_running:
            try:
                message = await remote.read_message()
            except EOFError:
                self.logger.debug(
                    'Endpoint %s got EOF in client handler for %s',
                    self.name,
                    remote,
                )
                break

            if isinstance(message, Broadcast):
                await self._message_queue.put((message.event, message.config))
            else:
                self.logger.warning(
                    'Endpoint %s received unexpected message: %s',
                    self.name,
                    message,
                )

    async def _connect_message_queue(self, group: curio.TaskGroup) -> None:
        self.logger.debug("Endpoint %s starting internal queue", self.name)
        await self._internal_loop_running.set()
        while self.is_running:
            (item, config) = await self._message_queue.get()
            await group.spawn(self._process_item, item, config)

    #
    # Primary endpoint API
    #
    async def broadcast(self, item: BaseEvent, config: Optional[BroadcastConfig] = None) -> None:
        """
        Broadcast an instance of :class:`~lahja.misc.BaseEvent` on the event bus. Takes
        an optional second parameter of :class:`~lahja.misc.BroadcastConfig` to decide
        where this event should be broadcasted to. By default, events are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        if not self.is_running:
            raise Exception("TODO: cannot broadcast without endpoint running")

        self.logger.debug('Broadcasting %s -> %s', item, config)
        item._origin = self.name

        if config is not None and config.internal:
            # Internal events simply bypass going through the central event bus
            # and are directly put into the local receiving queue instead.
            await self._message_queue.put((item, config))
        else:
            # Broadcast to every connected Endpoint that is allowed to receive the event
            remotes_for_broadcast = self._get_allowed_remotes_for_config(config)
            if not remotes_for_broadcast:
                self.logger.warning(
                    "Endpoint %s broadcast of %s with config %s matched 0/%d connected endpoints",
                    self.name,
                    item,
                    config,
                    len(self._connected_endpoints),
                )
            else:
                compressed_item = self._compress_event(item)
                async with curio.TaskGroup() as group:
                    for remote in remotes_for_broadcast:
                        await group.spawn(remote.send_message, Broadcast(compressed_item, config))

    def _get_allowed_remotes_for_config(self,
                                        config: Optional[BroadcastConfig],
                                        ) -> Tuple[RemoteEndpoint, ...]:
        if not self.is_running:
            raise Exception("TODO: cannot broadcast without endpoint running")

        if config is None:
            return tuple(self._connected_endpoints.values())

        return tuple(
            remote
            for name, remote in self._connected_endpoints.items()
            if config.allowed_to_receive(name)
        )

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
        if not self.is_running:
            raise Exception("TODO: cannot request without endpoint running")

        # TODO: shouldn't need to cast to a string...
        request_id = str(uuid.uuid4())

        item._origin = self.name
        item._id = request_id

        response_ready = curio.Event()

        # Create an asynchronous generator that we use to pipe the result
        agen = _wait_for_response(response_ready)
        # start the generator
        await agen.asend(None)
        self._pending_requests[request_id] = agen

        await self.broadcast(item, config)

        # Wait for the agen to set the ready event af
        await response_ready.wait()

        # The generator can/should only yield a single value which will be our
        # result.
        async for result in agen:
            expected_response_type = item.expected_response_type()
            if not isinstance(result, expected_response_type):
                raise UnexpectedResponse(
                    f"The type of the response is {type(result)}, expected: "
                    f"{expected_response_type}"
                )
            break

        return result

    TSubscribeEvent = TypeVar('TSubscribeEvent', bound=BaseEvent)

    def subscribe(self,
                  event_type: Type[TSubscribeEvent],
                  handler: Callable[[TSubscribeEvent], None]) -> Subscription:
        """
        Subscribe to receive updates for any event that matches the specified event type.
        A handler is passed as a second argument an :class:`~lahja.misc.Subscription` is returned
        to unsubscribe from the event if needed.
        """
        self._handlers[event_type].append(handler)
        return Subscription(lambda: self._handler[event_type].remove(handler))

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
        self.logger.debug(
            'Endpoint %s streaming event type: %s x %s',
            self.name,
            num_events,
            event_type,
        )
        if not self.is_running:
            raise Exception("TODO: cannot broadcast without endpoint running")

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
                next(counter)
            except StopIteration:
                break

            try:
                value = await queue.get()
                yield value
                # yield await queue.get()
            except GeneratorExit:
                break
            except Exception as err:
                self.logger.debug('Unhandled exception during event streaming: %s', err)
                raise
        self.logger.info('EXITING')

        # Because we use a `weakref.WeakSet` for storage the queue is
        # automatically removed when this function exits.

    TWaitForEvent = TypeVar('TWaitForEvent', bound=BaseEvent)

    async def wait_for(self, event_type: Type[TWaitForEvent]) -> TWaitForEvent:
        """
        Wait for a single instance of an event that matches the specified event type.
        """
        self.logger.info('Endpoint %s waiting for event type: %s', self.name, event_type)
        if not self.is_running:
            raise Exception("TODO: cannot broadcast without endpoint running")

        async for event in self.stream(event_type, num_events=1):
            return event


async def _wait_for_response(response_ready: curio.Event):
    response = yield
    await response_ready.set()
    # TODO: understand this and then document why this extra yield statement is
    # necessary...
    yield
    yield response


class EndpointServer:
    logger = logging.getLogger('lahja.curio.endpoint.EndpointServer')

    def __init__(self,
                 endpoint: CurioEndpoint,
                 ipc_path: Path) -> None:
        self.endpoint = endpoint
        self.ipc_path = ipc_path

        self._run_lock = curio.Lock()

        self._running = curio.Event()
        self._finished = curio.Event()

        self._should_stop_endpoint = False

    @classmethod
    def serve(cls, config: ConnectionConfig) -> CurioEndpoint:
        endpoint = CurioEndpoint(config.name)
        server = cls(endpoint, config.path)
        return server

    @property
    def is_running(self):
        """
        locked, running and not stopped
        """
        if not self._run_lock.locked():
            return False
        elif not self._running.is_set():
            return False
        elif self._finished.is_set():
            return False
        else:
            return True

    async def __aenter__(self) -> CurioEndpoint:
        await self.start()
        return self.endpoint

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_value: Optional[BaseException],
                        exc_tb: Optional[TracebackType]) -> None:
        self.logger.info('IN __aexit__ for %s', self.endpoint)
        await self.stop()

    async def start(self) -> None:
        """
        Start serving this :class:`~lahja.endpoint.Endpoint` so that it can receive events. Await
        until the :class:`~lahja.endpoint.Endpoint` is ready.
        """
        if self._finished.is_set():
            raise Exception("TODO: already ran")
        elif self._run_lock.locked():
            raise Exception("TODO: already starting")
        elif self._running.is_set():
            raise Exception("TODO: already running")

        if not self.endpoint.is_running:
            await self.endpoint.start()
            self._should_stop_endpoint = True

        self._external_loop_running = curio.Event()

        self._server_task = await curio.spawn(self._run_server)

        async with curio.TaskGroup() as group:
            await group.spawn(self._running.wait)
            await group.spawn(self._external_loop_running.wait)

    async def stop(self) -> None:
        """
        Stop the :class:`~lahja.endpoint.Endpoint` from receiving further events.
        """
        if not self._running.is_set():
            raise Exception("TODO: never ran")
        elif self._finished.is_set():
            raise Exception("TODO: already stopped")
        elif not self._run_lock.locked():
            raise Exception("TODO: not running...")

        async with curio.TaskGroup() as group:
            await group.spawn(self._server_task.cancel)
            await group.spawn(self._finished.wait)
        self.logger.debug(f"Server for endpoint %s stopped", self.endpoint)

        if self._should_stop_endpoint and self.endpoint.is_running:
            await self.endpoint.stop()

    async def _run_server(self) -> None:
        async with self._run_lock:
            self._external_queue = curio.Queue()

            await self._running.set()

            async with curio.TaskGroup() as group:
                await group.spawn(_clean_finished_tasks, group)
                await group.spawn(self._connect_external_queue, group)

                self.logger.debug("Server for endpoint %s starting", self.endpoint)
                try:
                    await curio.unix_server(
                        str(self.ipc_path),
                        functools.partial(self._accept_connection, group),
                    )
                except curio.TaskCancelled:
                    self.logger.info(
                        'Server for endpoint %s received cancellation',
                        self.endpoint,
                    )
                    pass
                finally:
                    self.logger.debug("Server for endpoint %s stopping", self.endpoint)
                    await group.cancel_remaining()

            del self._external_queue
            del self._external_loop_running
            self.ipc_path.unlink()

            await self._finished.set()

    async def _accept_connection(self,
                                 group: curio.TaskGroup,
                                 socket: curio.io.Socket,
                                 address: str) -> None:
        self.logger.info(
            'Server for endpoint %s accepting connection: %s @ %s',
            self.endpoint,
            socket,
            address,
        )
        remote = RemoteEndpoint(socket)

        # This lock ensures that the `group` doesn't close while we are still
        # trying to start the handler.
        if self._finished.is_set():
            self.logger.debug(
                "Server for endpoint %s aborting client connection to %s due to shutdown",
                self.endpoint,
                remote,
            )
            return
        else:
            task = await group.spawn(self._handle_client, remote)

        await task.join()

    async def _handle_client(self, remote: RemoteEndpoint) -> None:
        self.logger.info(
            'Server for endpoint %s starting client handler for %s',
            self.endpoint,
            remote
        )
        while self.is_running:
            try:
                message = await remote.read_message()
            except EOFError:
                self.logger.debug(
                    'Server for endpoint %s got EOF in client handler for %s',
                    self.endpoint,
                    remote,
                )
                break

            if isinstance(message, Broadcast):
                if isinstance(message.event, _Hello):
                    self.logger.debug(
                        "Server for endpoind %s got _Hello from %s:%s",
                        self.endpoint,
                        message.event._origin,
                        remote,
                    )
                    self.endpoint.direct_connect(message.event._origin, remote)
                await self._external_queue.put((message.event, message.config))
            else:
                self.logger.warning(
                    'Server for endpoint %s received unexpected message: %s',
                    self.endpoint,
                    message,
                )

    def _decompress_event(self, data: Union[BaseEvent, bytes]) -> BaseEvent:
        if isinstance(data, BaseEvent):
            return data
        else:
            import snappy
            return cast(BaseEvent, pickle.loads(snappy.decompress(data)))

    async def _connect_external_queue(self, group) -> None:
        await self._external_loop_running.set()
        while self.is_running:
            (item, config) = await self._external_queue.get()
            event = self._decompress_event(item)
            await group.spawn(self.endpoint._process_item, event, config)


serve = EndpointServer.serve
