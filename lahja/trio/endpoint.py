from abc import ABC, abstractmethod
import collections
import hashlib
import inspect
import itertools
import logging
import pathlib
import pickle
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    DefaultDict,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    cast,
)
import weakref

from async_generator import asynccontextmanager
import trio
import trio_typing

from lahja import constants
from lahja.base import (
    BaseEndpoint,
    BaseRemoteEndpoint,
    ConnectionAPI,
    TResponse,
    TStreamEvent,
    TSubscribeEvent,
    TWaitForEvent,
)
from lahja.common import (
    BaseEvent,
    BaseRequestResponseEvent,
    Broadcast,
    BroadcastConfig,
    ConnectionConfig,
    Message,
    Msg,
    Subscription,
    SubscriptionsAck,
    SubscriptionsUpdated,
    _MyNameIs,
    _WhoAreYou,
)
from lahja.exceptions import (
    ConnectionAttemptRejected,
    RemoteDisconnected,
    UnexpectedResponse,
)
from lahja.typing import RequestID


class Runnable(ABC):
    def __init__(self) -> None:
        self._running = trio.Event()
        self._stopped = trio.Event()

    async def wait_started(self) -> None:
        await self._running.wait()

    async def wait_stopped(self) -> None:
        await self.wait_started()
        await self._stopped.wait()

    @property
    def is_running(self) -> bool:
        return not self.is_stopped and self._running.is_set()

    @property
    def is_stopped(self) -> bool:
        return self._stopped.is_set()

    async def start(self, nursery: trio_typing.Nursery) -> None:
        if self._running.is_set():
            raise Exception("TODO: already ran")
        elif self.is_stopped:
            raise Exception("TODO: already stopped")

        nursery.start_soon(self._run)
        # this is a trade-off between allowing flexibility in how long it takes
        # for a `Runnable` to start and getting good errors in the event that
        # the `_run` method forgets to set the `_running` event.
        with trio.fail_after(2):
            await self.wait_started()

    @abstractmethod
    async def _run(self) -> None:
        """
        Must be implemented by subclasses.  This implementation **MUST** set
        the `self._running` event.
        """
        ...

    async def stop(self) -> None:
        if self.is_stopped:
            return
        elif not self.is_running:
            raise Exception("TODO: not running")

        self._stopped.set()
        await self._cleanup()

    @abstractmethod
    async def _cleanup(self) -> None:
        ...


class Connection(Runnable, ConnectionAPI):
    logger = logging.getLogger("lahja.trio.Connection")

    def __init__(self, socket: trio.SocketStream) -> None:
        self._socket = socket
        self._msg_send_channel, self._msg_receive_channel = cast(
            Tuple[trio.abc.SendChannel[Message], trio.abc.ReceiveChannel[Message]],
            trio.open_memory_channel(100),
        )
        super().__init__()

    def __str__(self) -> str:
        return f"Connection[{self._socket}]"

    def __repr__(self) -> str:
        return f"<{self}>"

    @classmethod
    async def connect_to(cls, path: pathlib.Path) -> "Connection":
        socket = await trio.open_unix_socket(str(path))
        cls.logger.debug("Opened connection to %s: %s", path, socket)
        return cls(socket)

    async def _run(self) -> None:
        async with trio.open_nursery() as nursery:
            self._nursery = nursery
            nursery.start_soon(self._read_stream)
            self._running.set()
            await self.wait_stopped()

    async def _cleanup(self) -> None:
        self._nursery.cancel_scope.cancel()
        await self._msg_send_channel.aclose()
        await self._socket.aclose()

    async def send_message(self, message: Msg) -> None:
        msg_data = pickle.dumps(message)
        size = len(msg_data)
        await self._socket.send_all(size.to_bytes(4, "little") + msg_data)

    async def _read_stream(self) -> None:
        """
        This method buffers the underlying socket, reading larger quantities of
        data from the socket at a time and then buffering the data internally
        to pull of the individual messages.
        """
        buffer = bytearray()

        while not self.is_stopped:
            while len(buffer) < 4:
                try:
                    data = await self._socket.receive_some(2048)
                except trio.ClosedResourceError:
                    return

                if data == b"":
                    raise RemoteDisconnected()
                buffer.extend(data)

            t_size = 4 + int.from_bytes(buffer[:4], "little")

            while len(buffer) < t_size:
                data = await self._socket.receive_some(2048)
                if data == b"":
                    raise RemoteDisconnected()
                buffer.extend(data)

            msg = cast(Message, pickle.loads(buffer[4:t_size]))
            await self._msg_send_channel.send(msg)
            buffer = buffer[t_size:]

    async def read_message(self) -> Message:
        return await self._msg_receive_channel.receive()


@asynccontextmanager
async def run_connection(connection: Connection) -> AsyncGenerator[Connection, None]:
    async with trio.open_nursery() as nursery:
        await connection.start(nursery)
        try:
            yield connection
        finally:
            await connection.stop()


class RemoteEndpoint(Runnable, BaseRemoteEndpoint):
    logger = logging.getLogger("lahja.trio.endpoint.RemoteEndpoint")

    _name: Optional[str]

    def __init__(
        self,
        name: Optional[str],
        conn: Connection,
        new_msg_fn: Callable[[Broadcast], Awaitable[Any]],
    ) -> None:
        self.name = name
        self._conn = conn
        self.new_msg_fn = new_msg_fn

        self.subscribed_messages: Set[Type[BaseEvent]] = set()

        self._subscriptions_updated = trio.Condition()
        self._received_ack = trio.Condition()

        super().__init__()

    def __str__(self) -> str:
        return f"Remote[{self.name if self.name is not None else self._conn}]"

    def __repr__(self) -> str:
        return f"<{self}>"

    async def _run(self) -> None:
        async with run_connection(self._conn):
            self._running.set()

            while self.is_running:
                try:
                    message = await self._conn.read_message()
                except RemoteDisconnected:
                    return

                if isinstance(message, Broadcast):
                    await self.new_msg_fn(message)
                elif isinstance(message, SubscriptionsAck):
                    async with self._received_ack:
                        self._received_ack.notify_all()
                elif isinstance(message, SubscriptionsUpdated):
                    async with self._subscriptions_updated:
                        self.subscribed_messages = message.subscriptions
                        self._subscriptions_updated.notify_all()
                        if message.response_expected:
                            await self.send_message(SubscriptionsAck())
                else:
                    self.logger.error(f"received unexpected message: {message}")

    async def _cleanup(self) -> None:
        async with self._received_ack:
            self._received_ack.notify_all()

    async def notify_subscriptions_updated(
        self, subscriptions: Set[Type[BaseEvent]], block: bool = True
    ) -> None:
        """
        Alert the ``Endpoint`` which has connected to us that our subscription set has
        changed. If ``block`` is ``True`` then this function will block until the remote
        endpoint has acknowledged the new subscription set. If ``block`` is ``False`` then this
        function will return immediately after the send finishes.
        """
        # The lock ensures only one coroutine can notify this endpoint at any one time
        # and that no replies are accidentally received by the wrong coroutines.
        async with self._received_ack:
            try:
                await self._conn.send_message(SubscriptionsUpdated(subscriptions, block))
            except RemoteDisconnected:
                return
            if block:
                await self._received_ack.wait()

    async def wait_until_subscription_received(self) -> None:
        async with self._subscriptions_updated:
            await self._subscriptions_updated.wait()


async def wait_for_path(path: trio.Path) -> None:
    """
    Wait for the path to appear at ``path``
    """
    while not await path.exists():
        await trio.sleep(0.05)


@asynccontextmanager
async def run_remote_endpoint(
    remote: RemoteEndpoint
) -> AsyncGenerator[RemoteEndpoint, None]:
    async with trio.open_nursery() as nursery:
        await remote.start(nursery)
        try:
            yield remote
        finally:
            await remote.stop()


class RemoteManager(Runnable):
    logger = logging.getLogger("lahja.trio.endpoint.RemoteManager")

    _connections: Set[RemoteEndpoint]

    def __init__(self, connections_changed: trio.Condition):
        self._connections = set()

        self._connections_changed = connections_changed

        super().__init__()

    async def _run(self) -> None:
        async with trio.open_nursery() as nursery:
            self._nursery = nursery
            self._running.set()
            try:
                await self._stopped.wait()
            except RemoteDisconnected:
                await self.stop()

    async def _cleanup(self) -> None:
        async with self._connections_changed:
            # TODO: evaluate if this is truly necessary to unblock things that
            # are waiting on this....
            self._connections_changed.notify_all()
        self._nursery.cancel_scope.cancel()

    async def _handle_endpoint(self, remote: RemoteEndpoint) -> None:
        self.logger.debug("Running remote %s", remote)

        async with run_remote_endpoint(remote):
            await remote.wait_stopped()

        async with self._connections_changed:
            self._connections.remove(remote)
            self._connections_changed.notify_all()

    #
    # Access to the remotes
    #
    def has_named_remote(self, name: str) -> bool:
        return any(remote.name == name for remote in self._connections)

    def get_named_remotes(self) -> Tuple[RemoteEndpoint, ...]:
        return tuple(remote for remote in self._connections if remote.name is not None)

    def get_anonymous_remotes(self) -> Tuple[RemoteEndpoint, ...]:
        return tuple(remote for remote in self._connections if remote.name is None)

    def get_all_remotes(self) -> Tuple[RemoteEndpoint, ...]:
        return tuple(self._connections)

    #
    # Starting remotes
    #
    async def start_remote(self, remote: RemoteEndpoint) -> None:
        if not self.is_running:
            raise Exception("Not running")
        self._nursery.start_soon(self._handle_endpoint, remote)
        await remote.wait_started()

    #
    # Public API for remote management
    #
    async def add_remote(self, remote: RemoteEndpoint) -> None:
        """
        Track and run a named `RemoteEndpoint`.
        """
        if remote in self._connections:
            raise ConnectionAttemptRejected(f"TODO: Already tracking remote: {remote}")
        elif not remote.is_running:
            raise ConnectionAttemptRejected(f"TODO: Remote is not running: {remote}")

        self.logger.debug("Adding remote: %s", remote)

        async with self._connections_changed:
            self._connections.add(remote)
            self._connections_changed.notify_all()


@asynccontextmanager
async def run_remote_manager(
    manager: RemoteManager
) -> AsyncGenerator[RemoteManager, None]:
    async with trio.open_nursery() as nursery:
        await manager.start(nursery)
        try:
            yield manager
        finally:
            await manager.stop()


ConcreteBroadcast = Tuple[BaseEvent, Optional[BroadcastConfig]]
WireBroadcastChannelPair = Tuple[
    trio.abc.SendChannel[Broadcast], trio.abc.ReceiveChannel[Broadcast]
]
ConcreteBroadcastChannelPair = Tuple[
    trio.abc.SendChannel[ConcreteBroadcast], trio.abc.ReceiveChannel[ConcreteBroadcast]
]
ConnectionChannelPair = Tuple[
    trio.abc.SendChannel[ConnectionConfig], trio.abc.ReceiveChannel[ConnectionConfig]
]
ResponseAgenType = AsyncGenerator[BaseEvent, BaseEvent]
StreamChannelPair = Tuple[
    trio.abc.SendChannel[BaseEvent], trio.abc.ReceiveChannel[BaseEvent]
]


class TrioEndpoint(Runnable, BaseEndpoint):
    _stream_channels: Dict[
        Type[BaseEvent], "weakref.WeakSet[trio.abc.SendChannel[BaseEvent]]"
    ]
    _pending_requests: Dict[RequestID, ResponseAgenType]

    _sync_handlers: DefaultDict[Type[BaseEvent], List[Callable[[TSubscribeEvent], Any]]]
    _async_handlers: DefaultDict[Type[BaseEvent], List[Callable[[TSubscribeEvent], Awaitable[Any]]]]

    _manager: RemoteManager

    logger = logging.getLogger("lahja.trio.TrioEndpoint")

    _ipc_path: trio.Path

    def __init__(self, name: str):
        self.name = name

        # TODO: DUPLICATED FUNCTIONALITY
        # normalize the request ID prefix to 6 bytes which combined with the
        # request_id counter gives a 1/16million chance of collision
        # TODO: this should be profiled with larger values to determine the performance overhead
        try:
            _name_digest = hashlib.sha256(self.name.encode("ascii")).digest()
        except UnicodeDecodeError:
            raise Exception(
                f"TODO: Invalid endpoint name: '{name}'. Must be ASCII encodable string"
            )
        self._request_id_base = _name_digest[:6]
        _counter_start = int.from_bytes(_name_digest[6:8], "little")
        # the counter starts somewhere in the 65536 range for additional base
        # randomness
        self._request_id_counter = itertools.count(_counter_start)

        # Temporary storage for SendChannels used in the `stream` API.  Uses a
        # `WeakSet` to automate cleanup.
        self._stream_channels = collections.defaultdict(weakref.WeakSet)
        self._pending_requests = {}
        self._sync_handlers = collections.defaultdict(list)
        self._async_handlers = collections.defaultdict(list)

        self._run_lock = trio.Lock()

        self._connections_changed = trio.Condition()
        self._manager = RemoteManager(self._connections_changed)

        # events used to signal that the endpoint has fully booted up.
        self._message_processing_loop_running = trio.Event()
        self._connection_loop_running = trio.Event()
        self._who_are_you_stream_handler_running = trio.Event()
        self._process_broadcasts_running = trio.Event()

        self._server_running = trio.Event()
        self._server_stopped = trio.Event()

        super().__init__()

    def __str__(self) -> str:
        return f"Endpoint[{self.name}]"

    def __repr__(self) -> str:
        return f"<{self}>"

    @asynccontextmanager
    async def run(self) -> AsyncIterator["TrioEndpoint"]:
        async with trio.open_nursery() as nursery:
            await self.start(nursery)
            try:
                yield self
            finally:
                await self.stop()

    @classmethod
    @asynccontextmanager
    async def serve(cls, config: ConnectionConfig) -> AsyncIterator["TrioEndpoint"]:
        async with cls(config.name).run() as endpoint:
            async with trio.open_nursery() as nursery:
                await endpoint._start_serving(nursery, trio.Path(config.path))
                try:
                    yield endpoint
                finally:
                    await endpoint._stop_serving()

    #
    # Running and endpoint
    #
    async def wait_started(self) -> None:
        await self._running.wait()
        await self._message_processing_loop_running.wait()
        await self._connection_loop_running.wait()
        await self._process_broadcasts_running.wait()

    async def _run(self) -> None:
        # This send/receive channel is fed by connected `RemoteEndpoint`
        # objects which feed received events into the send side of the channel.
        # Those messages are then retrieved from the receive channel by the
        # `_process_received_messages` daemon.
        (self._message_send_channel, message_receive_channel) = cast(
            WireBroadcastChannelPair, trio.open_memory_channel(100)
        )

        # This send/receive channel is fed by
        # `Endpoint.connect_to_endpoint` which places a 2-tuple of
        # (ConnectionConfig, trio.Event) which is retrieved by
        # `_process_connections`.
        (self._connection_send_channel, connection_receive_channel) = cast(
            ConnectionChannelPair, trio.open_memory_channel(100)
        )

        # This send/receive channel is fed by `Endpoint.broadcast_nowait`
        # which places a 2-tuple of (BaseEvent, BroadcastConfig) which is
        # retrieved by the `_process_broadcasts` and fed into the
        # `Endpoint.broadcast` API.
        (self._broadcast_send_channel, broadcast_receive_channel) = cast(
            ConcreteBroadcastChannelPair, trio.open_memory_channel(100)
        )

        self.logger.debug("%s: starting", self)

        # Register a handler for the `_WhoAreYou` message to respond with our
        # name using the main `EndpointAPI.subscribe` API.
        await self.subscribe(_WhoAreYou, self._handle_who_are_you)

        async with trio.open_nursery() as nursery:
            # store nursery on self so that we can either access it for
            # cancellation **or** use it to spawn the long running server
            # task.
            self._nursery = nursery

            self._running.set()

            #
            # _process_broadcasts:
            #     Manages a channel that `Endpoint.broadcast_nowait` places
            #     `Broadcast` messages on (typically from synchronous
            #     contexts).  These messages are fed into
            #     `Endpoint.broadcast`
            #
            nursery.start_soon(self._process_broadcasts, broadcast_receive_channel)

            #
            # _run_remote_manager:
            #     Runs the `RemoteManager` which is responsible for the
            #     lifecycle of all `RemoteEndpoint` objects
            nursery.start_soon(self._run_remote_manager)

            #
            # _process_received_messages:
            #     Manages a channel which all incoming
            #     event broadcasts are placed on, running each event through
            #     the internal `_process_item` handler which handles all of the
            #     internal logic for the various
            #     `request/response/subscribe/stream/wait_for` API.
            #
            nursery.start_soon(self._process_received_messages, message_receive_channel)

            #
            # _process_connections:
            #     When `Endpoint.connect_to_endpoint` is called, the actual
            #     connection process is done asynchronously by putting the
            #     `ConnectionConfig` onto a queue which this process
            #     retrieves to establish the new connection.  This includes
            #     handing the `RemoteEndpoint` oblect off to the `RemoteManager`
            #     which takes care of the connection lifecycle.
            #
            nursery.start_soon(self._process_connections, connection_receive_channel)

            await self.wait_stopped()

    async def _cleanup(self) -> None:
        self._nursery.cancel_scope.cancel()

        # Cleanup stateful things.
        await self._message_send_channel.aclose()
        await self._connection_send_channel.aclose()
        await self._broadcast_send_channel.aclose()

        del self._message_send_channel
        del self._connection_send_channel
        del self._broadcast_send_channel
        del self._nursery

        self.logger.debug("%s: stopped", self)

    #
    # Background processes
    #
    async def _process_broadcasts(
        self, channel: trio.abc.ReceiveChannel[ConcreteBroadcast]
    ) -> None:
        self._process_broadcasts_running.set()
        async for (item, config) in channel:
            await self.broadcast(item, config)

    async def _run_remote_manager(self) -> None:
        async with run_remote_manager(self._manager):
            await self._manager.wait_stopped()

    async def _handle_who_are_you(self, req: _WhoAreYou) -> None:
        """
        Long running process that responds to _WhoAreYou events which are
        typically send from server to client to establish a reverse connection.
        """
        await self.broadcast(_MyNameIs(self.name), req.broadcast_config())

    async def _process_connections(
        self, connection_receive_channel: trio.abc.ReceiveChannel[ConnectionConfig]
    ) -> None:
        """
        Long running process that establishes connections to endpoint servers
        and runs the handler for receiving events sent by the server over that
        connection.
        """
        self.logger.debug("%s: starting new connection channel", self)
        self._connection_loop_running.set()
        async for config in connection_receive_channel:
            # Allow some time for for the IPC socket to appear
            with trio.fail_after(constants.IPC_WAIT_SECONDS):
                await wait_for_path(trio.Path(config.path))

            # Establish the socket connection
            connection = await Connection.connect_to(config.path)

            # Create the remote
            remote = RemoteEndpoint(
                config.name,
                connection,
                self._message_send_channel.send,
            )

            # Let the manager start and run the remote
            await self._manager.start_remote(remote)
            await self._manager.add_remote(remote)

            # Block until we show as being connected to the remote.
            await self.wait_until_connected_to(config.name)

    async def _process_received_messages(
        self, receive_channel: trio.abc.ReceiveChannel[Broadcast]
    ) -> None:
        """
        Consume events that have been received from a remote endpoint and
        process them.
        """
        self._message_processing_loop_running.set()
        async for (item, config) in receive_channel:
            event = self._decompress_event(item)
            self._nursery.start_soon(self._process_item, event, config)

    #
    # Establishing connections
    #
    async def connect_to_endpoints(self, *endpoints: ConnectionConfig) -> None:
        """
        Connect to the given endpoints and await until all connections are established.
        """
        # TODO: need tests
        async with trio.open_nursery() as nursery:
            for config in endpoints:
                nursery.start_soon(self.connect_to_endpoint, config)

    async def connect_to_endpoint(self, config: ConnectionConfig) -> None:
        """
        Establish a connection to a named endpoint server over an IPC socket.
        """
        if not self.is_running:
            raise Exception(
                "TODO: cannot establish connections if endpoint isn't running"
            )

        # Ensure we are not already connected to the named endpoint
        if self._manager.has_named_remote(config.name):
            raise ConnectionAttemptRejected(
                f"Already connected to endpoint with name: {config.name}"
            )

        # Feed the `ConnectionConfig` through a channel where
        # `_process_connections` will pick it up and actually establish the
        # connection.
        await self._connection_send_channel.send(config)

        # Allow some time for for the IPC socket to appear
        with trio.fail_after(constants.CONNECT_TO_ENDPOINT_TIMEOUT):
            # We wait until the *wait* APIs register us as having connected to
            # the endpoint.
            await self.wait_until_connected_to(config.name)

    async def wait_until_connections_change(self) -> None:
        async with self._connections_changed:
            await self._connections_changed.wait()

    @property
    def subscribed_events(self) -> Set[Type[BaseEvent]]:
        """
        Return the set of events this Endpoint is currently listening for
        """
        return set(
            self._sync_handlers.keys()
        ) | set(
            self._async_handlers.keys()
        ) | set(
            self._stream_channels.keys()
        )

    async def _notify_subscriptions_changed(self) -> None:
        """
        Tell all inbound connections of our new subscriptions
        """
        async with trio.open_nursery() as nursery:
            subscribed_events = self.subscribed_events
            # make a copy so that the set doesn't change while we iterate over it
            for remote in self._manager.get_all_remotes():
                nursery.start_soon(
                    remote.notify_subscriptions_updated, subscribed_events
                )

    def get_connected_endpoints_and_subscriptions(
        self
    ) -> Tuple[Tuple[Optional[str], Set[Type[BaseEvent]]], ...]:
        """
        Return all connected endpoints and their event type subscriptions to this endpoint.
        """
        return tuple(
            (outbound.name, outbound.subscribed_messages)
            for outbound in self._manager.get_all_remotes()
        )

    #
    # Internal event processing logic
    #
    async def _process_item(self, item: BaseEvent, config: BroadcastConfig) -> None:
        event_type = type(item)

        has_config = config is not None

        # handle request/response
        in_pending_requests = (
            has_config and config.filter_event_id in self._pending_requests
        )

        if in_pending_requests:
            agen = self._pending_requests.pop(config.filter_event_id)  # type: ignore
            await agen.asend(item)

        # handle stream channel
        if event_type in self._stream_channels:
            channels = tuple(self._stream_channels[event_type])
            for send_channel in channels:
                try:
                    await send_channel.send(item)
                except trio.ClosedResourceError:
                    self._stream_channels[event_type].remove(send_channel)

        # handle subscriptions
        if event_type in self._sync_handlers:
            for handler_fn in self._sync_handlers[event_type]:
                try:
                    handler_fn(item)
                except Exception as err:
                    self.logger.debug(
                        "%s: handler function %s error: %s", self, handler_fn, err
                    )
        if event_type in self._async_handlers:
            for handler_fn in self._async_handlers[event_type]:
                try:
                    await handler_fn(item)
                except Exception as err:
                    self.logger.debug(
                        "%s: handler function %s error: %s", self, handler_fn, err
                    )

    #
    # Primary endpoint API
    #
    async def broadcast(
        self, item: BaseEvent, config: Optional[BroadcastConfig] = None
    ) -> None:
        """
        Broadcast an instance of :class:`~lahja.common.BaseEvent` on the event bus. Takes
        an optional second parameter of :class:`~lahja.common.BroadcastConfig` to decide
        where this event should be broadcasted to. By default, events are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        if config is not None and config.internal:
            # Internal events simply bypass going through the central event bus
            # and are directly put into the local receiving queue instead.
            raise NotImplementedError("Not implemented")

        # Broadcast to every connected Endpoint that is allowed to receive the event
        remotes_for_broadcast = self._get_allowed_remotes_for_config(config)
        if not remotes_for_broadcast:
            self.logger.debug(
                "%s: broadcast of %s with config %s matched 0/%d connected endpoints",
                self,
                item,
                config,
                len(self._manager.get_named_remotes()),
            )
        else:
            await self._send_message_to_remotes(
                item, config, *remotes_for_broadcast
            )

    async def _broadcast(
        self,
        item: BaseEvent,
        config: Optional[BroadcastConfig],
        id: Optional[RequestID],
        remotes_for_broadcast: Optional[Tuple[RemoteEndpoint, ...]],
    ) -> None:
        """
        Broadcast an instance of :class:`~lahja.common.BaseEvent` on the event bus. Takes
        an optional second parameter of :class:`~lahja.common.BroadcastConfig` to decide
        where this event should be broadcasted to. By default, events are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        item.bind(self, id)

        if config is not None and config.internal:
            # Internal events simply bypass going through the central event bus
            # and are directly put into the local receiving queue instead.
            raise NotImplementedError("Not implemented")
            await self._message_queue.put((item, config))

        if remotes_for_broadcast is None:
            await self._send_message_to_remotes(
                item, config, *self._get_allowed_remotes_for_config(config)
            )
        else:
            await self._send_message_to_remotes(item, config, *remotes_for_broadcast)

    def broadcast_nowait(
        self, item: BaseEvent, config: Optional[BroadcastConfig] = None
    ) -> None:
        self._broadcast_send_channel.send_nowait((item, config))

    async def _send_message_to_remotes(
        self,
        item: BaseEvent,
        config: Optional[BroadcastConfig],
        *remotes: RemoteEndpoint,
    ) -> None:
        # mark the origin of the item when sending to a remote endpoint
        # TODO: this mechanism is messy/imprecises/error-prone
        compressed_item = self._compress_event(item)
        message = Broadcast(compressed_item, config)
        for remote in remotes:
            try:
                await remote.send_message(message)
            except (trio.ClosedResourceError, trio.BrokenResourceError) as err:
                self.logger.debug(
                    "%s: dropping disconnected remote %s: %s", remote, err
                )
                await remote.stop()

    def _get_allowed_remotes_for_config(
        self, config: Optional[BroadcastConfig]
    ) -> Tuple[RemoteEndpoint, ...]:
        # TODO: DUPLICATED FUNCTIONALITY
        candidates = self._manager.get_named_remotes()
        if config is None:
            return candidates

        return tuple(
            remote
            for remote in candidates
            if config.allowed_to_receive(remote.name)
            if remote.name is not None
        )

    TResponse = TypeVar("TResponse", bound=BaseEvent)

    async def request(
        self,
        item: BaseRequestResponseEvent[TResponse],
        config: Optional[BroadcastConfig] = None,
    ) -> TResponse:
        """
        Broadcast an instance of
        :class:`~lahja.common.BaseRequestResponseEvent` on the event bus and
        immediately wait on an expected answer of type
        :class:`~lahja.common.BaseEvent`. Optionally pass a second parameter of
        :class:`~lahja.common.BroadcastConfig` to decide where the request
        should be broadcasted to. By default, requests are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        if not self.is_running:
            raise Exception("TODO: cannot request without endpoint running")
        return await self._request(item, config, None)

    async def _request(
        self,
        item: BaseRequestResponseEvent[TResponse],
        config: Optional[BroadcastConfig],
        remotes_for_broadcast: Optional[Tuple[RemoteEndpoint, ...]],
    ) -> TResponse:
        # TODO: shouldn't need to cast to a string...
        request_id = self._get_request_id()

        response_ready = trio.Event()

        # Create an asynchronous generator that we use to pipe the result
        agen = _wait_for_response(response_ready)
        # start the generator
        await agen.asend(None)  # type: ignore  # mypy doesn't understand this
        self._pending_requests[request_id] = agen

        await self._broadcast(item, config, request_id, remotes_for_broadcast)

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

        return result  # type: ignore  # mypy doesn't recognize type

    TSubscribeEvent = TypeVar("TSubscribeEvent", bound=BaseEvent)

    async def subscribe(
        self,
        event_type: Type[TSubscribeEvent],
        handler: Callable[[TSubscribeEvent], Any],
    ) -> Subscription:
        """
        Subscribe to receive updates for any event that matches the specified event type.
        A handler is passed as a second argument an :class:`~lahja.common.Subscription` is returned
        to unsubscribe from the event if needed.
        """
        if inspect.iscoroutinefunction(handler):
            self._async_handlers[event_type].append(handler)
            subscription = Subscription(lambda: self._async_handlers[event_type].remove(handler))
        else:
            self._sync_handlers[event_type].append(handler)
            subscription = Subscription(lambda: self._sync_handlers[event_type].remove(handler))
        # TODO: subscribe -> async
        # notify subscriptions have been updated.
        await self._notify_subscriptions_changed()
        return subscription

    TStreamEvent = TypeVar("TStreamEvent", bound=BaseEvent)

    async def stream(
        self, event_type: Type[TStreamEvent], num_events: Optional[int] = None
    ) -> AsyncGenerator[TStreamEvent, None]:
        """
        Stream all events that match the specified event type. This returns an
        ``AsyncIterable[BaseEvent]`` which can be consumed through an ``async for`` loop.
        An optional ``num_events`` parameter can be passed to stop streaming after a maximum amount
        of events was received.
        """
        if not self.is_running:
            raise Exception("TODO: cannot broadcast without endpoint running")

        # TODO: buffer-size of 0 means `send(...)` calls block until the thing
        # has been received
        (send_channel, receive_channel) = cast(
            StreamChannelPair, trio.open_memory_channel(100)
        )

        self._stream_channels[event_type].add(send_channel)

        # notify subscriptions have been updated.
        await self._notify_subscriptions_changed()

        if num_events is None:
            # iterate forever
            counter = itertools.count()
        else:
            # fixed number of iterations
            counter = iter(range(max(0, num_events - 1)))

        async for event in receive_channel:
            yield event  # type: ignore  # mypy doesn't recognize this having a correct type

            try:
                next(counter)
            except StopIteration:
                await send_channel.aclose()
                break

        # Because we use a `weakref.WeakSet` for storage the queue is
        # automatically removed when this function exits.

    TWaitForEvent = TypeVar("TWaitForEvent", bound=BaseEvent)

    async def wait_for(self, event_type: Type[TWaitForEvent]) -> TWaitForEvent:
        """
        Wait for a single instance of an event that matches the specified event type.
        """
        if not self.is_running:
            raise Exception("TODO: cannot broadcast without endpoint running")

        agen = self.stream(event_type, num_events=1)
        event = await agen.asend(None)
        await agen.aclose()
        return event

    #
    # Serving
    #
    @property
    def is_serving(self) -> bool:
        return self._server_running.is_set() and not self._server_stopped.is_set()

    async def _start_serving(self, nursery: trio_typing. Nursery, ipc_path: trio.Path) -> None:
        if not self.is_running:
            raise Exception("TODO: endpoint must be running")
        elif self.is_serving:
            raise Exception("TODO: endpoint already serving")

        self.ipc_path = ipc_path
        nursery.start_soon(self._run_server, ipc_path)

        # Wait until the ipc socket has appeared and is accepting connections.
        with trio.fail_after(constants.IPC_WAIT_SECONDS):
            await wait_for_path(ipc_path)

    async def _stop_serving(self) -> None:
        if self._server_stopped.is_set():
            return

        self._server_stopped.set()
        self._server_nursery.cancel_scope.cancel()

        del self._server_nursery

        try:
            await self.ipc_path.unlink()
        except OSError:
            pass
        self.logger.debug(f"%s: server stopped", self)

    async def _run_server(self, ipc_path: trio.Path) -> None:
        self._server_running.set()

        async with trio.open_nursery() as nursery:
            # Store nursery on self so that we can access it for cancellation
            self._server_nursery = nursery

            self.logger.debug("%s: server starting", self)
            # TODO: is this really the best way to open a unix socket and listen!!!
            socket = trio.socket.socket(trio.socket.AF_UNIX, trio.socket.SOCK_STREAM)
            await socket.bind(self.ipc_path.__fspath__())
            # TODO: the 1 here might be too small.
            socket.listen(1)
            listener = trio.SocketListener(socket)
            try:
                await trio.serve_listeners(
                    handler=self._handle_client,
                    listeners=(listener,),
                    handler_nursery=nursery,
                )
            finally:
                # TODO: maybe closing the socket should happen here instead
                # of having the `RemoteEndpoint` be in charge of closing
                # it.
                self.logger.debug("%s: server stopping", self)

    async def _handle_client(self, socket: trio.SocketStream) -> None:
        self.logger.debug("%s: starting client handler for %s", self, socket)
        connection = Connection(socket)
        remote = RemoteEndpoint(None, connection, self._message_send_channel.send)
        await self._manager.start_remote(remote)
        await self._manager.add_remote(remote)
        self._server_nursery.start_soon(self._establish_reverse_connection, remote)

        # block until the remote stops.  Without this the socket gets closed on
        # us automatically by the trio internals that call this method.
        await remote.wait_stopped()

    async def _establish_reverse_connection(self, remote: RemoteEndpoint) -> None:
        # Initiate the handshake to establish a reverse connection.

        with trio.move_on_after(constants.REVERSE_CONNECT_TIMEOUT) as cancel_scope:
            # TODO: stop using private endpoint API
            response = await self._request(_WhoAreYou(), None, (remote,))

            if self._manager.has_named_remote(response.name):
                self.logger.debug(
                    "%s: already connected to %s.  aborting reverse connection",
                    self,
                    response.name,
                )
                return
            else:
                self.logger.debug(
                    "%s: established reverse connection to client %s",
                    self,
                    response.name,
                )
                remote.name = response.name
                async with self._connections_changed:
                    self._connections_changed.notify_all()
        if cancel_scope.cancelled_caught:
            self.logger.error(
                (
                    "%s: failed to establish reverse connection "
                    "with connecting client"
                ),
                self,
            )


async def _wait_for_response(response_ready: trio.Event) -> ResponseAgenType:
    response = yield  # type: ignore  # mypy doesn't recognize the types at play here
    response_ready.set()
    # TODO: understand this and then document why this extra yield statement is
    # necessary...
    yield  # type: ignore  # mypy doesn't recognize the types at play here
    yield response
