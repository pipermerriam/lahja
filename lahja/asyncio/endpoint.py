import asyncio
from asyncio import StreamReader, StreamWriter
from collections import defaultdict
import functools
import hashlib
import inspect
import itertools
import logging
from pathlib import Path
import pickle
import time
import traceback
from typing import (  # noqa: F401
    Any,
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    DefaultDict,
    Dict,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from async_generator import asynccontextmanager

from lahja.base import (
    BaseEndpoint,
    BaseRemoteEndpoint,
    ConnectionAPI,
    TResponse,
    TStreamEvent,
    TSubscribeEvent,
)
from lahja.common import (
    BaseEvent,
    BaseRequestResponseEvent,
    Broadcast,
    BroadcastConfig,
    ConnectionConfig,
    Message,
    Msg,
    RemoteSubscriptionChanged,
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


async def wait_for_path(path: Path, timeout: int = 2) -> None:
    """
    Wait for the path to appear at ``path``
    """
    start_at = time.monotonic()
    while time.monotonic() - start_at < timeout:
        if path.exists():
            return
        await asyncio.sleep(0.05)

    raise TimeoutError(f"IPC socket file {path} has not appeared in {timeout} seconds")


SIZE_MARKER_LENGTH = 4


class Connection(ConnectionAPI):
    logger = logging.getLogger("lahja.endpoint.asyncio.Connection")

    def __init__(self, reader: StreamReader, writer: StreamWriter) -> None:
        self.writer = writer
        self.reader = reader
        self._drain_lock = asyncio.Lock()

    @classmethod
    async def connect_to(cls, path: Path) -> "Connection":
        reader, writer = await asyncio.open_unix_connection(str(path))
        return cls(reader, writer)

    async def send_message(self, message: Msg) -> None:
        pickled = pickle.dumps(message)
        size = len(pickled)

        try:
            self.writer.write(size.to_bytes(SIZE_MARKER_LENGTH, "little") + pickled)
            async with self._drain_lock:
                # Use a lock to serialize drain() calls. Circumvents this bug:
                # https://bugs.python.org/issue29930
                await self.writer.drain()
        except (BrokenPipeError, ConnectionResetError):
            raise RemoteDisconnected()
        except RuntimeError as err:
            # We don't do a pre-check for a closed writer since this is a performance critical
            # path. We also don't want to swallow runtime errors unrelated to closed handlers.
            if "handler is closed" in str(err):
                self.logger.warning("Failed to send %s. Handler closed.", message)
                raise RemoteDisconnected from err
            raise

    async def read_message(self) -> Message:
        if self.reader.at_eof():
            raise RemoteDisconnected()

        try:
            raw_size = await self.reader.readexactly(SIZE_MARKER_LENGTH)
            size = int.from_bytes(raw_size, "little")
            message = await self.reader.readexactly(size)
            obj = cast(Message, pickle.loads(message))
            return obj
        except (asyncio.IncompleteReadError, BrokenPipeError, ConnectionResetError):
            raise RemoteDisconnected()


class RemoteEndpoint(BaseRemoteEndpoint):
    """
    Represents a connection to another endpoint.  Connections *can* be
    bi-directional with messages flowing in either direction.

    A 'message' can be any of:

    - ``SubscriptionsUpdated``
            broadcasting the subscriptions that the endpoint on the other side
            of this connection is interested in.
    - ``SubscriptionsAck``
            acknowledgedment of a ``SubscriptionsUpdated``
    - ``Broadcast``
            an event meant to be processed by the endpoint.
    """

    logger = logging.getLogger("lahja.endpoint.asyncio.RemoteEndpoint")

    def __init__(
        self,
        name: Optional[str],
        conn: Connection,
        new_msg_func: Callable[[Broadcast], Awaitable[Any]],
    ) -> None:
        self.name = name
        self._conn = conn
        self.new_msg_func = new_msg_func

        self.subscribed_messages: Set[Type[BaseEvent]] = set()

        self._notify_lock = asyncio.Lock()

        self._received_response = asyncio.Condition()
        self._received_subscription = asyncio.Condition()

        self._running = asyncio.Event()
        self._stopped = asyncio.Event()

    def __str__(self) -> str:
        return f"RemoteEndpoint[{self.name if self.name is not None else id(self)}]"

    def __repr__(self) -> str:
        return f"<{self}>"

    async def wait_started(self) -> None:
        await self._running.wait()

    async def wait_stopped(self) -> None:
        await self._stopped.wait()

    async def is_running(self) -> bool:
        return not self.is_stopped and self.running.is_set()

    async def is_stopped(self) -> bool:
        return self._stopped.is_set()

    async def start(self) -> None:
        self._task = asyncio.ensure_future(self._run())
        await self.wait_started()

    async def stop(self) -> None:
        if self.is_stopped:
            return
        self._stopped.set()
        self._task.cancel()

    async def _run(self) -> None:
        # TODO: if we can properly define the *Condition* API, this
        # implementation can be made common between the asyncio and trio
        # implementations.
        self._running.set()

        while self.is_running:
            try:
                message = await self._conn.read_message()
            except RemoteDisconnected:
                async with self._received_response:
                    self._received_response.notify_all()
                return

            if isinstance(message, Broadcast):
                await self.new_msg_func(message)
            elif isinstance(message, SubscriptionsAck):
                async with self._received_response:
                    self._received_response.notify_all()
            elif isinstance(message, SubscriptionsUpdated):
                async with self._received_subscription:
                    self.subscribed_messages = message.subscriptions
                    self._received_subscription.notify_all()
                    if message.response_expected:
                        await self.send_message(SubscriptionsAck())
            else:
                self.logger.error(f"received unexpected message: {message}")

    async def notify_subscriptions_updated(
        self, subscriptions: Set[Type[BaseEvent]], block: bool = True
    ) -> None:
        """
        Alert the ``Endpoint`` which has connected to us that our subscription set has
        changed. If ``block`` is ``True`` then this function will block until the remote
        endpoint has acknowledged the new subscription set. If ``block`` is ``False`` then this
        function will return immediately after the send finishes.
        """
        # The extra lock ensures only one coroutine can notify this endpoint at any one time
        # and that no replies are accidentally received by the wrong
        # coroutines. Without this, in the case where `block=True`, this inner
        # block would release the lock on the call to `wait()` which would
        # allow the ack from a different update to incorrectly result in this
        # returning before the ack had been received.
        async with self._notify_lock:
            async with self._received_response:
                try:
                    await self._conn.send_message(
                        SubscriptionsUpdated(subscriptions, block)
                    )
                except RemoteDisconnected:
                    return
                if block:
                    await self._received_response.wait()

    async def wait_until_subscription_received(self) -> None:
        async with self._received_subscription:
            await self._received_subscription.wait()


@asynccontextmanager  # type: ignore
async def run_remote_endpoint(remote: RemoteEndpoint) -> AsyncIterable[RemoteEndpoint]:
    await remote.start()
    try:
        yield remote
    finally:
        await remote.stop()


TFunc = TypeVar("TFunc", bound=Callable[..., Any])


SubscriptionAsyncHandler = Callable[[BaseEvent], Awaitable[Any]]
SubscriptionSyncHandler = Callable[[BaseEvent], Any]


class AsyncioEndpoint(BaseEndpoint):
    """
    The :class:`~lahja.endpoint.asyncio.AsyncioEndpoint` enables communication
    between different processes as well as within a single process via various
    event-driven APIs.
    """

    _ipc_path: Path

    _receiving_queue: "asyncio.Queue[Tuple[Union[bytes, BaseEvent], Optional[BroadcastConfig]]]"
    _receiving_loop_running: asyncio.Event

    _subscription_updates_running: asyncio.Event
    _subscription_updates_condition: asyncio.Condition
    _subscription_updates_queue: "asyncio.Queue[None]"

    _futures: Dict[RequestID, "asyncio.Future[BaseEvent]"]

    _connections: Set[RemoteEndpoint]

    _async_handler: DefaultDict[Type[BaseEvent], List[SubscriptionAsyncHandler]]
    _sync_handler: DefaultDict[Type[BaseEvent], List[SubscriptionSyncHandler]]

    _loop: Optional[asyncio.AbstractEventLoop] = None

    def __init__(self, name: str) -> None:
        self.name = name

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

        # storage containers for inbound and outbound connections to other
        # endpoints
        self._connections = set()

        # storage for futures which are waiting for a response.
        self._futures: Dict[RequestID, "asyncio.Future[BaseEvent]"] = {}

        # handlers for event subscriptions.  These are
        # intentionally stored separately so that the cost of
        # `inspect.iscoroutine` is incurred once when the subscription is
        # created instead of for each event that is processed
        self._async_handler = defaultdict(list)
        self._sync_handler = defaultdict(list)

        # queues for stream handlers
        self._queues: Dict[Type[BaseEvent], List["asyncio.Queue[BaseEvent]"]] = {}

        # background tasks that are started as part of the process of running
        # the endpoint.
        self._endpoint_tasks: Set["asyncio.Future[Any]"] = set()

        # background tasks that are started as part of serving the endpoint
        # over an IPC socket.
        self._server_tasks: Set["asyncio.Future[Any]"] = set()

        # way to signal that the connections to other endpoints have changed
        self._connections_changed = asyncio.Condition()

        self._running = False
        self._serving = False

    def _get_request_id(self) -> RequestID:
        request_id = next(self._request_id_counter) % 65536
        return cast(RequestID, self._request_id_base + request_id.to_bytes(2, "little"))

    def __str__(self) -> str:
        return f"Endpoint[{self.name}]"

    def __repr__(self) -> str:
        return f"<{self.name}>"

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def is_serving(self) -> bool:
        return self._serving

    @property
    def ipc_path(self) -> Path:
        return self._ipc_path

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            raise AttributeError("Endpoint does not have an event loop set.")

        return self._loop

    def check_event_loop(func: TFunc) -> TFunc:  # type: ignore
        """
        All Endpoint methods must be called from the same event loop.
        """

        @functools.wraps(func)
        def run(self, *args, **kwargs):  # type: ignore
            if not self._loop:
                self._loop = asyncio.get_event_loop()

            if self._loop != asyncio.get_event_loop():
                raise Exception(
                    "All endpoint methods must be called from the same event loop"
                )

            return func(self, *args, **kwargs)

        return cast(TFunc, run)

    @check_event_loop
    async def start(self) -> None:
        if self.is_running:
            raise RuntimeError(f"Endpoint {self.name} is already running")

        self._receiving_loop_running = asyncio.Event()
        self._receiving_queue = asyncio.Queue()

        self._subscription_updates_running = asyncio.Event()
        self._subscription_updates_condition = asyncio.Condition()
        self._subscription_updates_queue = asyncio.Queue()

        self._running = True

        # background tasks
        self._endpoint_tasks.add(asyncio.ensure_future(self._connect_receiving_queue()))
        self._endpoint_tasks.add(
            asyncio.ensure_future(self._process_subscription_updates())
        )
        self._endpoint_tasks.add(
            asyncio.ensure_future(self._process_subscription_updates_queue())
        )

        # handle requests from servers sending the _WhoAreYou internal message,
        # responding with a _MyNameIs response.
        await self.subscribe(_WhoAreYou, self._handle_who_are_you)

        await self._receiving_loop_running.wait()
        await self._subscription_updates_running.wait()

        self.logger.debug("Endpoint[%s]: running", self.name)

    async def _process_subscription_updates_queue(self) -> None:
        while self.is_running:
            await self._subscription_updates_queue.get()
            async with self._subscription_updates_condition:
                self._subscription_updates_condition.notify_all()

    async def _process_subscription_updates(self) -> None:
        self._subscription_updates_running.set()
        async with self._subscription_updates_condition:
            while self.is_running:
                await self._subscription_updates_condition.wait()
                subscribed_events = self.subscribed_events
                await asyncio.gather(
                    *(
                        remote.notify_subscriptions_updated(subscribed_events)
                        for remote in self._connections
                    )
                )

    def _notify_subscriptions_changed_nowait(self) -> None:
        self._subscription_updates_queue.put_nowait(None)

    async def _handle_who_are_you(self, req: _WhoAreYou) -> None:
        """
        Handle incoming requests from connected endpoints for the name of this
        endpoint.
        """
        await self.broadcast(_MyNameIs(self.name), req.broadcast_config())

    async def _connect_receiving_queue(self) -> None:
        self._receiving_loop_running.set()
        while self.is_running:
            try:
                (item, config) = await self._receiving_queue.get()
            except RuntimeError as err:
                # do explicit check since RuntimeError is a bit generic and we
                # only want to catch the closed event loop case here.
                if str(err) == "Event loop is closed":
                    break
                raise
            try:
                event = self._decompress_event(item)
                await self._process_item(event, config)
            except Exception:
                traceback.print_exc()

    @check_event_loop
    async def start_server(self, ipc_path: Path) -> None:
        """
        Start serving this :class:`~lahja.endpoint.asyncio.AsyncioEndpoint` so that it
        can receive events. Await until the
        :class:`~lahja.endpoint.asyncio.AsyncioEndpoint` is ready.
        """
        if not self.is_running:
            raise RuntimeError(f"Endpoint {self.name} must be running to start server")
        elif self.is_serving:
            raise RuntimeError(f"Endpoint {self.name} is already serving")

        self._ipc_path = ipc_path

        self._serving = True

        self._server = await asyncio.start_unix_server(
            self._accept_conn, path=str(self.ipc_path)
        )
        self.logger.debug("Endpoint[%s]: server started", self.name)

    async def _accept_conn(self, reader: StreamReader, writer: StreamWriter) -> None:
        conn = Connection(reader, writer)
        remote = RemoteEndpoint(None, conn, self._receiving_queue.put)

        task = asyncio.ensure_future(self._handle_client(remote))
        task.add_done_callback(self._server_tasks.remove)
        task.add_done_callback(lambda _: self._connections.remove(remote))
        self._server_tasks.add(task)

        await remote.wait_started()

        # we **must** ensure that the subscription updates are locked between
        # the time that we manually update this individual connection and that
        # we place it within the set of tracked connections, otherwise, a
        # subscription update from elsewhere can occur between the time these
        # two statements execute resulting in the remote missing a new
        # subscription update.  Note that inverting these statements should
        # also mitigate this, but it has the downside of the manual update
        # potentially being redundant.
        async with self._subscription_updates_condition:
            await remote.notify_subscriptions_updated(self.subscribed_events)
            await self._add_connection(remote)

        self._server_tasks.add(
            asyncio.ensure_future(self._establish_reverse_connection(remote))
        )

    async def _handle_client(self, remote: RemoteEndpoint) -> None:
        async with run_remote_endpoint(remote):
            await remote.wait_stopped()

    async def _establish_reverse_connection(self, remote: RemoteEndpoint) -> None:
        try:
            response = await asyncio.wait_for(
                self._request(_WhoAreYou(), None, (remote,)), timeout=2
            )
        except asyncio.TimeoutError:
            self.logger.debug(
                "%s: failed to establish reverse connection to %s", self, remote
            )
        else:
            self.logger.debug(
                "%s: establish reverse connection to %s", self, response.name
            )
            # NOTE: this is a very *implicit* mechanism for upgrading a
            # connection.  As things currently stand, simply setting the name
            # results in all of the mechanisms that filter by name
            # automatically picking up on this as a named endpoint and applying
            # the appropriate filters.
            async with self._connections_changed:
                remote.name = response.name
                self._connections_changed.notify_all()

    @property
    def subscribed_events(self) -> Set[Type[BaseEvent]]:
        """
        Return the set of events this Endpoint is currently listening for
        """
        return (
            set(self._sync_handler.keys())
            .union(self._async_handler.keys())
            .union(self._queues.keys())
        )

    def get_connected_endpoints_and_subscriptions(
        self
    ) -> Tuple[Tuple[Optional[str], Set[Type[BaseEvent]]], ...]:
        """
        Return all connected endpoints and their event type subscriptions to this endpoint.
        """
        return tuple(
            (outbound.name, outbound.subscribed_messages)
            for outbound in self._connections
        )

    def _throw_if_already_connected(self, *endpoints: ConnectionConfig) -> None:
        seen: Set[str] = set()

        for config in endpoints:
            if config.name in seen:
                raise ConnectionAttemptRejected(
                    f"Trying to connect to {config.name} twice. Names must be uniqe."
                )
            elif self.is_connected_to(config.name):
                raise ConnectionAttemptRejected(
                    f"Already connected to {config.name} at {config.path}. Names must be unique."
                )
            else:
                seen.add(config.name)

    @check_event_loop
    async def connect_to_endpoints(self, *endpoints: ConnectionConfig) -> None:
        """
        Connect to the given endpoints and await until all connections are established.
        """
        self._throw_if_already_connected(*endpoints)
        await asyncio.gather(
            *(self._await_connect_to_endpoint(endpoint) for endpoint in endpoints)
        )

    def connect_to_endpoints_nowait(self, *endpoints: ConnectionConfig) -> None:
        """
        Connect to the given endpoints as soon as they become available but do not block.
        """
        self._throw_if_already_connected(*endpoints)
        for endpoint in endpoints:
            asyncio.ensure_future(self._await_connect_to_endpoint(endpoint))

    async def _await_connect_to_endpoint(self, endpoint: ConnectionConfig) -> None:
        await wait_for_path(endpoint.path)
        # sleep for a moment to give the server time to be ready to accept
        # connections
        await asyncio.sleep(0.01)
        await self.connect_to_endpoint(endpoint)

    async def connect_to_endpoint(self, config: ConnectionConfig) -> None:
        self._throw_if_already_connected(config)

        conn = await Connection.connect_to(config.path)
        remote = RemoteEndpoint(config.name, conn, self._receiving_queue.put)

        task = asyncio.ensure_future(self._handle_server(remote))
        # start a background task to watch for subscription changes and relay
        # them to the core *wait* APIS for monitoring which endpoints we are
        # connected to as well as their subscriptions.
        subscriptions_task = asyncio.ensure_future(
            self.watch_outbound_subscriptions(remote)
        )
        # upon completion this task should remove itself from the set of
        # tracked tasks.
        subscriptions_task.add_done_callback(self._endpoint_tasks.remove)
        self._endpoint_tasks.add(subscriptions_task)

        # remove the tasks from the set of tracked tasks.
        task.add_done_callback(self._endpoint_tasks.remove)
        # at the point where this task is done, so should the
        # `subscriptions_task` since we no longer need to watch for changes to
        # this remote's subscriptions
        task.add_done_callback(lambda _: subscriptions_task.cancel())
        # finally this remote can be removed from the set of tracked
        # connections.
        task.add_done_callback(lambda _: self._connections.remove(remote))
        self._endpoint_tasks.add(task)

        await remote.wait_started()

        # we **must** ensure that the subscription updates are locked between
        # the time that we manually update this individual connection and that
        # we place it within the set of tracked connections, otherwise, a
        # subscription update from elsewhere can occur between the time these
        # two statements execute resulting in the remote missing a new
        # subscription update.  Note that inverting these statements should
        # also mitigate this, but it has the downside of the manual update
        # potentially being redundant.
        async with self._subscription_updates_condition:
            await remote.notify_subscriptions_updated(self.subscribed_events)
            await self._add_connection(remote)

    async def wait_until_connections_change(self) -> None:
        async with self._connections_changed:
            await self._connections_changed.wait()

    async def _handle_server(self, remote: RemoteEndpoint) -> None:
        async with run_remote_endpoint(remote):
            await remote.wait_stopped()

    async def watch_outbound_subscriptions(self, outbound: RemoteEndpoint) -> None:
        while outbound in self._connections:
            await outbound.wait_until_subscription_received()
            await self.broadcast(
                RemoteSubscriptionChanged(), BroadcastConfig(internal=True)
            )

    async def _add_connection(self, remote: RemoteEndpoint) -> None:
        if remote in self._connections:
            raise Exception("TODO: remote is already tracked")
        elif remote.name is not None and self.is_connected_to(remote.name):
            raise Exception(
                f"TODO: already connected to remote with name {remote.name}"
            )

        async with self._connections_changed:
            self._connections.add(remote)
            self._connections_changed.notify_all()

    async def _process_item(
        self, item: BaseEvent, config: Optional[BroadcastConfig]
    ) -> None:
        event_type = type(item)

        if config is not None and config.filter_event_id in self._futures:
            future = self._futures[config.filter_event_id]
            if not future.done():
                future.set_result(item)
            self._futures.pop(config.filter_event_id, None)

        if event_type in self._queues:
            for queue in self._queues[event_type]:
                queue.put_nowait(item)

        if event_type in self._sync_handler:
            for handler in self._sync_handler[event_type]:
                handler(item)

        if event_type in self._async_handler:
            await asyncio.gather(
                *(handler(item) for handler in self._async_handler[event_type])
            )

    def stop_server(self) -> None:
        if not self.is_serving:
            return
        self._serving = False

        self._server.close()

        for task in self._server_tasks:
            task.cancel()

        self.ipc_path.unlink()
        self.logger.debug("Endpoint[%s]: server stopped", self.name)

    def stop(self) -> None:
        """
        Stop the :class:`~lahja.endpoint.asyncio.AsyncioEndpoint` from receiving further events.
        """
        if not self.is_running:
            return

        self.stop_server()

        self._running = False

        for task in self._endpoint_tasks:
            task.cancel()

        self.logger.debug("Endpoint[%s]: stopped", self.name)

    @asynccontextmanager
    async def run(self) -> AsyncIterator["AsyncioEndpoint"]:
        if not self._loop:
            self._loop = asyncio.get_event_loop()

        await self.start()

        try:
            yield self
        finally:
            self.stop()

    @classmethod
    @asynccontextmanager
    async def serve(cls, config: ConnectionConfig) -> AsyncIterator["AsyncioEndpoint"]:
        endpoint = cls(config.name)
        async with endpoint.run():
            await endpoint.start_server(config.path)
            yield endpoint

    async def broadcast(
        self, item: BaseEvent, config: Optional[BroadcastConfig] = None
    ) -> None:
        """
        Broadcast an instance of :class:`~lahja.common.BaseEvent` on the event bus. Takes
        an optional second parameter of :class:`~lahja.common.BroadcastConfig` to decide
        where this event should be broadcasted to. By default, events are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        await self._broadcast(item, config, None, None)

    async def _broadcast(
        self,
        item: BaseEvent,
        config: Optional[BroadcastConfig],
        id: Optional[RequestID],
        remotes_for_broadcast: Optional[Tuple[RemoteEndpoint, ...]],
    ) -> None:
        item.bind(self, id)

        if config is not None and config.internal:
            # Internal events simply bypass going over the event bus and are
            # processed immediately.
            await self._process_item(item, config)
            return

        # Broadcast to every connected Endpoint that is allowed to receive the event
        if remotes_for_broadcast is None:
            await self._send_to_remotes(
                item, config, *self._get_remotes_for_broadcast(item, config)
            )
        else:
            await self._send_to_remotes(item, config, *remotes_for_broadcast)

    def _get_remotes_for_broadcast(
        self, item: BaseEvent, config: Optional[BroadcastConfig]
    ) -> Tuple[RemoteEndpoint, ...]:
        return tuple(
            remote for remote in self._connections if remote.can_send_item(item, config)
        )

    async def _send_to_remotes(
        self,
        item: BaseEvent,
        config: Optional[BroadcastConfig],
        *remotes: RemoteEndpoint,
    ) -> None:
        if not item.is_bound:
            raise Exception("TODO: item must be bound")
        compressed_item = self._compress_event(item)

        message = Broadcast(compressed_item, config)
        for remote in remotes:
            try:
                await remote.send_message(message)
            except RemoteDisconnected:
                self.logger.debug(f"Remote endpoint {remote} no longer exists")
                self._connections.remove(remote)

    def broadcast_nowait(
        self, item: BaseEvent, config: Optional[BroadcastConfig] = None
    ) -> None:
        """
        A non-async :meth:`~lahja.Endpoint.broadcast` (see :meth:`~lahja.Endpoint.broadcast`
        for more)

        Instead of blocking the calling coroutine this function schedules the broadcast
        and immediately returns.

        CAUTION: You probably don't want to use this. broadcast() doesn't return until the
        write socket has finished draining, meaning that the OS has accepted the message.
        This prevents us from sending more data than the remote process can handle.
        broadcast_nowait has no such backpressure. Even after the remote process stops
        accepting new messages this function will continue to accept them, which in the
        worst case could lead to runaway memory usage.
        """
        asyncio.ensure_future(self._broadcast(item, config, None, None))

    @check_event_loop
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
        return await self._request(item, config, None)

    async def _request(
        self,
        item: BaseRequestResponseEvent[TResponse],
        config: Optional[BroadcastConfig],
        remotes_for_broadcast: Optional[Tuple[RemoteEndpoint, ...]],
    ) -> TResponse:
        request_id = self._get_request_id()

        future: "asyncio.Future[TResponse]" = asyncio.Future()
        self._futures[request_id] = future  # type: ignore
        # mypy can't reconcile the TResponse with the declared type of
        # `self._futures`.

        self.logger.info("SENDING TO: %s", remotes_for_broadcast)
        await self._broadcast(item, config, request_id, remotes_for_broadcast)

        future.add_done_callback(
            functools.partial(self._remove_cancelled_future, request_id)
        )

        result = await future

        expected_response_type = item.expected_response_type()
        if not isinstance(result, expected_response_type):
            raise UnexpectedResponse(
                f"The type of the response is {type(result)}, expected: {expected_response_type}"
            )

        return result

    def _remove_cancelled_future(
        self, id: RequestID, future: "asyncio.Future[Any]"
    ) -> None:
        try:
            future.exception()
        except asyncio.CancelledError:
            self._futures.pop(id, None)

    def _remove_async_subscription(
        self, event_type: Type[BaseEvent], handler_fn: SubscriptionAsyncHandler
    ) -> None:
        self._async_handler[event_type].remove(handler_fn)
        if not self._async_handler[event_type]:
            self._async_handler.pop(event_type)
        # this is asynchronous because that's a better user experience than making
        # the user `await subscription.remove()`. This means this Endpoint will keep
        # getting events for a little while after it stops listening for them but
        # that's a performance problem, not a correctness problem.
        self._notify_subscriptions_changed_nowait()

    def _remove_sync_subscription(
        self, event_type: Type[BaseEvent], handler_fn: SubscriptionSyncHandler
    ) -> None:
        self._sync_handler[event_type].remove(handler_fn)
        if not self._sync_handler[event_type]:
            self._sync_handler.pop(event_type)
        # this is asynchronous because that's a better user experience than making
        # the user `await subscription.remove()`. This means this Endpoint will keep
        # getting events for a little while after it stops listening for them but
        # that's a performance problem, not a correctness problem.
        self._notify_subscriptions_changed_nowait()

    async def subscribe(
        self,
        event_type: Type[TSubscribeEvent],
        handler: Callable[[TSubscribeEvent], Union[Any, Awaitable[Any]]],
    ) -> Subscription:
        """
        Subscribe to receive updates for any event that matches the specified event type.
        A handler is passed as a second argument an :class:`~lahja.common.Subscription` is returned
        to unsubscribe from the event if needed.
        """
        if inspect.iscoroutinefunction(handler):
            casted_handler = cast(SubscriptionAsyncHandler, handler)
            self._async_handler[event_type].append(casted_handler)
            unsubscribe_fn = functools.partial(
                self._remove_async_subscription, event_type, casted_handler
            )
        else:
            casted_handler = cast(SubscriptionSyncHandler, handler)
            self._sync_handler[event_type].append(casted_handler)
            unsubscribe_fn = functools.partial(
                self._remove_sync_subscription, event_type, casted_handler
            )

        async with self._subscription_updates_condition:
            self._subscription_updates_condition.notify_all()

        return Subscription(unsubscribe_fn)

    async def stream(
        self, event_type: Type[TStreamEvent], num_events: Optional[int] = None
    ) -> AsyncGenerator[TStreamEvent, None]:
        """
        Stream all events that match the specified event type. This returns an
        ``AsyncIterable[BaseEvent]`` which can be consumed through an ``async for`` loop.
        An optional ``num_events`` parameter can be passed to stop streaming after a maximum amount
        of events was received.
        """
        queue: "asyncio.Queue[TStreamEvent]" = asyncio.Queue()
        casted_queue = cast("asyncio.Queue[BaseEvent]", queue)
        if event_type not in self._queues:
            self._queues[event_type] = []

        self._queues[event_type].append(casted_queue)

        async with self._subscription_updates_condition:
            self._subscription_updates_condition.notify_all()

        if num_events is None:
            # loop forever
            iterations = itertools.repeat(True)
        else:
            iterations = itertools.repeat(True, num_events)

        try:
            for _ in iterations:
                try:
                    yield await queue.get()
                except GeneratorExit:
                    break
                except asyncio.CancelledError:
                    break
        finally:
            self._queues[event_type].remove(casted_queue)
            if not self._queues[event_type]:
                del self._queues[event_type]
            # use nowait here since removing a subscription is not time
            # sensitive and not blocking here is better performance.
            self._notify_subscriptions_changed_nowait()
