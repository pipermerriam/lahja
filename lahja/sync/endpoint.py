from contextlib import contextmanager
import logging
import socket
import pickle
from pathlib import Path
import threading
from typing import cast, Type, Set, Callable, Any, Optional, Union, TypeVar, Tuple, Iterator

from lahja import constants
from lahja._snappy import check_has_snappy_support
from lahja.base import ConnectionAPI
from lahja.common import (
    BaseEvent,
    BaseRequestResponseEvent,
    Hello,
    Broadcast,
    BroadcastConfig,
    ConnectionConfig,
    Message,
    Msg,
    Subscription,
    SubscriptionsUpdated,
    SubscriptionsAck,
    should_endpoint_receive_item,
    RequestIDGenerator,
)
from lahja.exceptions import ConnectionAttemptRejected, RemoteDisconnected
from lahja.typing import RequestID


class BlockingConnection:
    def __init__(self, sock):
        self._socket = sock
        self._lock = threading.Lock()
        self._buffer = bytearray

    @classmethod
    def connect_to(cls, path: Path) -> ConnectionAPI:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(path)
        return cls(sock)

    def send_message(self, message: Msg) -> None:
        pickled = pickle.dumps(message)
        size = len(pickled)
        with self._lock:
            self._socket.sendall(size.to_bytes(constants.SIZE_MARKER_LENGTH, "little") + pickled)

    def _read_exactly(self, num_bytes) -> bytes:
        while len(self._buffer) < num_bytes:
            self._buffer.extend(self._socket.recv(4096))
        payload = bytes(self._buffer[:num_bytes])
        self._buffer = self._buffer[num_bytes:]
        return payload

    def read_message(self) -> Message:
        with self._lock:
            raw_size = self._read_exactly(4)
            size = int.from_bytes(raw_size, "little")
            message = self._read_exactly(size)
        obj = cast(Message, pickle.loads(message))
        return obj


class BlockingRemoteEndpoint:
    logger = logging.getLogger("lahja.base.RemoteEndpoint")

    conn: ConnectionAPI

    _local_name: str
    _running: threading.Event
    _ready: threading.Event
    _stopped: threading.Event

    _notify_lock: threading.Lock

    _received_response: threading.Condition
    _received_subscription: threading.Condition

    _subscribed_events: Set[Type[BaseEvent]]

    _subscriptions_initialized: threading.Event

    def __init__(
        self,
        local_name: str,
        conn: ConnectionAPI,
        new_msg_func: Callable[[Broadcast], Any],
    ) -> None:
        self._local_name = local_name
        self.conn = conn
        self.new_msg_func = new_msg_func

        self._subscribed_events = set()

        self._running = threading.Event()
        self._ready = threading.Event()
        self._stopped = threading.Event()

    def __str__(self) -> str:
        return f"RemoteEndpoint[{self._name if self._name is not None else id(self)}]"

    def __repr__(self) -> str:
        return f"<{self}>"

    _name: Optional[str] = None

    @property
    def name(self) -> str:
        if self._name is None:
            raise AttributeError("Endpoint name has not yet been established")
        else:
            return self._name

    def wait_started(self) -> None:
        self._running.wait()

    def wait_ready(self) -> None:
        self._ready.wait()

    def wait_stopped(self) -> None:
        self._stopped.wait()

    @property
    def is_running(self) -> bool:
        return not self.is_stopped and self._running.is_set()

    @property
    def is_ready(self) -> bool:
        return self.is_running and self._ready.is_set()

    @property
    def is_stopped(self) -> bool:
        return self._stopped.is_set()

    def _process_incoming_messages(self) -> None:
        self._running.set()

        # Send the hello message
        self.send_message(Hello(self._local_name))

        # Wait for the other endpoint to identify itself.
        hello = self.conn.read_message()
        if isinstance(hello, Hello):
            self._name = hello.name
            self.logger.debug(
                "RemoteEndpoint connection established: %s <-> %s",
                self._local_name,
                self.name,
            )
            self._ready.set()
        else:
            self.logger.debug(
                "Invalid message: First message must be `Hello`, Got: %s", hello
            )
            self._stopped.set()
            return

        while self.is_running:
            try:
                message = self.conn.read_message()
            except RemoteDisconnected:
                with self._received_response:
                    self._received_response.notify_all()
                return

            if isinstance(message, Broadcast):
                self.new_msg_func(message)
            elif isinstance(message, SubscriptionsUpdated):
                self._subscriptions_initialized.set()
                with self._received_subscription:
                    self._subscribed_events = message.subscriptions
                    self._received_subscription.notify_all()
                # The ack is sent after releasing the lock since we've already
                # exited the code which actually updates the subscriptions and
                # we are merely responding to the sender to acknowledge
                # receipt.
                if message.response_expected:
                    self.send_message(SubscriptionsAck())
            elif isinstance(message, SubscriptionsAck):
                with self._received_response:
                    self._received_response.notify_all()
            else:
                self.logger.error(f"received unexpected message: {message}")

    def get_subscribed_events(self) -> Set[Type[BaseEvent]]:
        return self._subscribed_events

    def notify_subscriptions_updated(
        self, subscriptions: Set[Type[BaseEvent]], block: bool = True
    ) -> None:
        """
        Alert the endpoint on the other side of this connection that the local
        subscriptions have changed. If ``block`` is ``True`` then this function
        will block until the remote endpoint has acknowledged the new
        subscription set. If ``block`` is ``False`` then this function will
        return immediately after the send finishes.
        """
        # The extra lock ensures only one coroutine can notify this endpoint at any one time
        # and that no replies are accidentally received by the wrong
        # coroutines. Without this, in the case where `block=True`, this inner
        # block would release the lock on the call to `wait()` which would
        # allow the ack from a different update to incorrectly result in this
        # returning before the ack had been received.
        with self._notify_lock:
            with self._received_response:
                try:
                    self.conn.send_message(
                        SubscriptionsUpdated(subscriptions, block)
                    )
                except RemoteDisconnected:
                    return
                if block:
                    self._received_response.wait()

    def send_message(self, message: Msg) -> None:
        self.conn.send_message(message)

    def wait_until_subscription_initialized(self) -> None:
        self._subscriptions_initialized.wait()


TWaitForEvent = TypeVar("TWaitForEvent", bound=BaseEvent)


class BlockingEndpoint:
    _remote_connections_changed: threading.Condition
    _remote_subscriptions_changed: threading.Condition

    _connections: Set[BlockingRemoteEndpoint]

    _get_request_id: Iterator[RequestID]

    logger = logging.getLogger("lahja.endpoint.Endpoint")

    def __init__(self, name: str) -> None:
        self.name = name

        try:
            self._get_request_id = RequestIDGenerator(name.encode("ascii") + b":")
        except UnicodeDecodeError:
            raise Exception(
                f"TODO: Invalid endpoint name: '{name}'. Must be ASCII encodable string"
            )

        # storage containers for inbound and outbound connections to other
        # endpoints
        self._connections = set()

        self._running = threading.Event()
        self._stopped = threading.Event()
        self._serving = threading.Event()

        self._receiving_loop_running = threading.Event()
        self._receiving_queue = threading.Queue()

    def __str__(self) -> str:
        return f"Endpoint[{self.name}]"

    def __repr__(self) -> str:
        return f"<{self.name}>"

    @property
    def is_running(self) -> bool:
        return self._running.is_set()

    @property
    def is_serving(self) -> bool:
        return self._serving.is_set()

    def wait_started(self) -> None:
        self._running.wait()

    @property
    def ipc_path(self) -> Path:
        return self._ipc_path

    #
    # Running lifecycle
    #
    @contextmanager
    def run(self) -> Iterator['BlockingEndpoint']:
        self._start()

        try:
            yield self
        finally:
            self._stop()

    def _start(self) -> None:
        self._thread = threading.Thread(
            target=self._run,
        )
        self.wait_started()

    def _stop(self) -> None:
        self._stopped.set()
        # TODO: stopping threads not easy...
        self._thread.join(2)

    def _run(self) -> None:
        self._running = True

        receiving_queue_thread = threading.Thread(target=self._connect_receiving_queue)
        receiving_queue_thread.start()
        self._endpoint_tasks.add(receiving_queue_thread)

        subscriptions_changed_thread = threading.Thread(target=self._monitor_subscription_changes)
        subscriptions_changed_thread.start()
        self._endpoint_tasks.add(subscriptions_changed_thread)

        self._receiving_loop_running.wait()
        self.logger.debug("Endpoint[%s]: running", self.name)

    def _connect_receiving_queue(self) -> None:
        self._receiving_loop_running.set()
        while self.is_running:
            try:
                (item, config) = self._receiving_queue.get()
            except RuntimeError as err:
                # do explicit check since RuntimeError is a bit generic and we
                # only want to catch the closed event loop case here.
                if str(err) == "Event loop is closed":
                    break
                raise
            try:
                event = self._decompress_event(item)
                self._process_item(event, config)
            except Exception:
                traceback.print_exc()

    def _monitor_subscription_changes(self) -> None:
        while self.is_running:
            # We wait for the event to change and then immediately replace it
            # with a new event.  This **must** occur before any additional
            # `await` calls to ensure that any *new* changes to the
            # subscriptions end up operating on the *new* event and will be
            # picked up in the next iteration of the loop.
            self._subscriptions_changed.wait()
            self._subscriptions_changed = asyncio.Event()

            # make a copy so that the set doesn't change while we iterate
            # over it
            subscribed_events = self.get_subscribed_events()

            with self._remote_connections_changed:
                for remote in self._connections:
                    remote.notify_subscriptions_updated(
                        subscribed_events, block=False
                    )
            with self._remote_subscriptions_changed:
                self._remote_subscriptions_changed.notify_all()

    def _process_item(
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
                queue.put(item)

        if event_type in self._sync_handler:
            for handler in self._handlers[event_type]:
                try:
                    handler(item)
                except Exception:
                    self.logger.debug(
                        "%s: Error in subscription handler %s",
                        self,
                        handler,
                        exc_info=True,
                    )
    #
    # Server API
    #
    @classmethod
    @contextmanager
    def serve(cls, config: ConnectionConfig) -> Iterator["BlockingEndpoint"]:
        endpoint = cls(config.name)
        with endpoint.run():
            endpoint._start_server(config.path)
            try:
                yield endpoint
            finally:
                endpoint._stop_server()

    def _start_server(self, ipc_path: Path) -> None:
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

        self._server = threading.Thread(
            target=_run_server,
        )
        self._serving.wait()
        self.logger.debug("Endpoint[%s]: server started", self.name)

    def _stop_server(self) -> None:
        if not self.is_serving:
            return
        self._server_stopped.set()
        self._server.join(2)

        self.ipc_path.unlink()
        self.logger.debug("Endpoint[%s]: server stopped", self.name)

    def _run_server(self, ipc_path: Path) -> None:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.bind((HOST, PORT))
            sock.listen(1)
            while self.is_serving:
                conn, addr = sock.accept()
                self.logger.debug('Connection from: %s', addr)
                # TODO: track thread
                threading.Thread(
                    target=self._accept_conn,
                    args=(conn,),
                ).start()

        async def _accept_conn(self, sock: socket.socket) -> None:
            with sock:
                conn = BlockingConnection(sock)
                remote = BlockingRemoteEndpoint(
                    local_name=self.name,
                    conn=conn,
                    subscriptions_changed=self._remote_subscriptions_changed,
                    new_msg_func=self._receiving_queue.put,
                )
                with remote.run():
                    remote.wait_stopped()

    #
    # Common implementations
    #
    def __reduce__(self) -> None:  # type: ignore
        raise NotImplementedError("Endpoints cannot be pickled")

    #
    # Connection API
    #
    def connect_to_endpoints(self, *endpoints: ConnectionConfig) -> None:
        for config in endpoints:
            self.connect_to_endpoint(config)

    def is_connected_to(self, endpoint_name: str) -> bool:
        if endpoint_name == self.name:
            return True
        return any(endpoint_name == remote.name for remote in self._connections)

    def wait_until_connected_to(self, endpoint_name: str) -> None:
        """
        Return once a connection exists to an endpoint with the given name.
        """
        with self._remote_connections_changed:
            while True:
                if self.is_connected_to(endpoint_name):
                    return
                self._remote_connections_changed.wait()

    def get_connected_endpoints_and_subscriptions(
        self
    ) -> Tuple[Tuple[str, Set[Type[BaseEvent]]], ...]:
        """
        Return all connected endpoints and their event type subscriptions to this endpoint.
        """
        return ((self.name, self.get_subscribed_events()),) + tuple(
            (remote.name, remote.get_subscribed_events())
            for remote in self._connections
        )

    def wait_until_connections_change(self) -> None:
        """
        Block until the set of connected remote endpoints changes.
        """
        with self._remote_connections_changed:
            self._remote_connections_changed.wait()

    #
    # Event API
    #
    def wait_for(self, event_type: Type[TWaitForEvent]) -> TWaitForEvent:
        """
        Wait for a single instance of an event that matches the specified event type.
        """
        gen = self.stream(event_type, num_events=1)
        event = gen.send(None)
        gen.close()
        return event

    #
    # Subscription API
    #
    def wait_until_endpoint_subscriptions_change(self) -> None:
        with self._remote_subscriptions_changed:
            self._remote_subscriptions_changed.wait()

    def is_endpoint_subscribed_to(
        self, remote_endpoint: str, event_type: Type[BaseEvent]
    ) -> bool:
        """
        Return ``True`` if the specified remote endpoint is subscribed to the specified event type
        from this endpoint. Otherwise return ``False``.
        """
        for endpoint, subscriptions in self.get_connected_endpoints_and_subscriptions():
            if endpoint != remote_endpoint:
                continue

            for subscribed_event_type in subscriptions:
                if subscribed_event_type is event_type:
                    return True

        return False

    def is_any_endpoint_subscribed_to(self, event_type: Type[BaseEvent]) -> bool:
        """
        Return ``True`` if at least one of the connected remote endpoints is subscribed to the
        specified event type from this endpoint. Otherwise return ``False``.
        """
        for endpoint, subscriptions in self.get_connected_endpoints_and_subscriptions():
            for subscribed_event_type in subscriptions:
                if subscribed_event_type is event_type:
                    return True

        return False

    def are_all_endpoints_subscribed_to(
        self, event_type: Type[BaseEvent], include_self: bool = True
    ) -> bool:
        """
        Return ``True`` if every connected remote endpoint is subscribed to the specified event
        type from this endpoint. Otherwise return ``False``.
        """
        for endpoint, subscriptions in self.get_connected_endpoints_and_subscriptions():
            if not include_self and endpoint == self.name:
                continue

            if event_type not in subscriptions:
                return False

        return True

    def wait_until_endpoint_subscribed_to(
        self, remote_endpoint: str, event: Type[BaseEvent]
    ) -> None:
        """
        Block until the specified remote endpoint has subscribed to the specified event type
        from this endpoint.
        """
        with self._remote_subscriptions_changed:
            while True:
                if self.is_endpoint_subscribed_to(remote_endpoint, event):
                    return
                self._remote_subscriptions_changed.wait()

    def wait_until_any_endpoint_subscribed_to(
        self, event: Type[BaseEvent]
    ) -> None:
        """
        Block until any other remote endpoint has subscribed to the specified event type
        from this endpoint.
        """
        with self._remote_subscriptions_changed:
            while True:
                if self.is_any_endpoint_subscribed_to(event):
                    return
                self._remote_subscriptions_changed.wait()

    def wait_until_all_endpoints_subscribed_to(
        self, event: Type[BaseEvent], *, include_self: bool = True
    ) -> None:
        """
        Block until all currently connected remote endpoints are subscribed to the specified
        event type from this endpoint.
        """
        with self._remote_subscriptions_changed:
            while True:
                if self.are_all_endpoints_subscribed_to(
                    event, include_self=include_self
                ):
                    return
                self._remote_subscriptions_changed.wait()

    #
    # Compression
    #

    # This property gets assigned during class creation.  This should be ok
    # since snappy support is defined as the module being importable and that
    # should not change during the lifecycle of the python process.
    has_snappy_support = check_has_snappy_support()

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

    #
    # Remote Endpoint Management
    #
    def _run_remote_endpoint(self, remote: BlockingRemoteEndpoint) -> None:
        with remote.run():
            remote.wait_ready()
            self._add_connection(remote)
            remote.wait_until_subscription_initialized()
            remote.wait_stopped()

        if remote in self._connections:
            self._remove_connection(remote)

    def _add_connection(self, remote: BlockingRemoteEndpoint) -> None:
        if remote in self._connections:
            raise ConnectionAttemptRejected("Remote is already tracked")
        elif self.is_connected_to(remote.name):
            raise ConnectionAttemptRejected(
                f"Already connected to remote with name {remote.name}"
            )

        with self._remote_connections_changed:
            # then add them to our set of connections and signal that both
            # the remote connections and subscriptions have been updated.
            remote.notify_subscriptions_updated(self.get_subscribed_events())
            self._connections.add(remote)
            self._remote_connections_changed.notify_all()
        with self._remote_subscriptions_changed:
            self._remote_subscriptions_changed.notify_all()

    def _remove_connection(self, remote: BlockingRemoteEndpoint) -> None:
        with self._remote_connections_changed:
            self._connections.remove(remote)
            self._remote_connections_changed.notify_all()
        with self._remote_subscriptions_changed:
            self._remote_subscriptions_changed.notify_all()
