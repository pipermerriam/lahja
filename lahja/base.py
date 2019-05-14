from abc import ABC, abstractmethod
import itertools
import logging
from pathlib import Path
import pickle
from typing import (  # noqa: F401
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
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

from ._snappy import check_has_snappy_support
from .common import (
    BaseEvent,
    BaseRequestResponseEvent,
    BroadcastConfig,
    ConnectionConfig,
    Message,
    Msg,
    RemoteSubscriptionChanged,
    Subscription,
)
from .typing import RequestID

TResponse = TypeVar("TResponse", bound=BaseEvent)
TWaitForEvent = TypeVar("TWaitForEvent", bound=BaseEvent)
TSubscribeEvent = TypeVar("TSubscribeEvent", bound=BaseEvent)
TStreamEvent = TypeVar("TStreamEvent", bound=BaseEvent)


class ConnectionAPI(ABC):
    @classmethod
    @abstractmethod
    async def connect_to(cls, path: Path) -> "ConnectionAPI":
        ...

    @abstractmethod
    async def send_message(self, message: Msg) -> None:
        ...

    @abstractmethod
    async def read_message(self) -> Message:
        ...


class RemoteEndpointAPI(ABC):
    name: Optional[str]
    subscribed_messages: Set[Type[BaseEvent]]
    _conn: ConnectionAPI

    @abstractmethod
    async def notify_subscriptions_updated(
        self, subscriptions: Set[Type[BaseEvent]], block: bool = True
    ) -> None:
        """
        Alert the ``Endpoint`` which has connected to us that our subscription set has
        changed. If ``block`` is ``True`` then this function will block until the remote
        endpoint has acknowledged the new subscription set. If ``block`` is ``False`` then this
        function will return immediately after the send finishes.
        """
        ...

    @abstractmethod
    def can_send_item(self, item: BaseEvent, config: Optional[BroadcastConfig]) -> bool:
        ...

    @abstractmethod
    async def send_message(self, message: Msg) -> None:
        ...

    @abstractmethod
    async def wait_until_subscription_received(self) -> None:
        ...


class BaseRemoteEndpoint(RemoteEndpointAPI):
    def can_send_item(self, item: BaseEvent, config: Optional[BroadcastConfig]) -> bool:
        if config is not None:
            if self.name is not None and not config.allowed_to_receive(self.name):
                return False
            elif config.filter_event_id is not None:
                # the item is a response to a request.
                return True

        return type(item) in self.subscribed_messages

    async def send_message(self, message: Msg) -> None:
        await self._conn.send_message(message)


class EndpointAPI(ABC):
    """
    The :class:`~lahja.endpoint.Endpoint` enables communication between different processes
    as well as within a single process via various event-driven APIs.
    """

    __slots__ = ("name",)

    name: str

    @property
    @abstractmethod
    def is_running(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_serving(self) -> bool:
        ...

    #
    # Running and Server API
    #
    @abstractmethod
    def run(self) -> AsyncContextManager["BaseEndpoint"]:
        """
        Context manager API for running endpoints.

        .. code-block::

            async with endpoint.run() as endpoint:
                ... # endpoint running within context
            ... # endpoint stopped after

        test
        """
        ...

    @classmethod
    @abstractmethod
    def serve(cls, config: ConnectionConfig) -> AsyncContextManager["BaseEndpoint"]:
        """
        Context manager API for running and endpoint server.

        .. code-block::

            async with endpoint.serve():
                ... # server running within context
            ... # server stopped

        """
        ...

    #
    # Connection API
    #
    @abstractmethod
    async def connect_to_endpoint(self, config: ConnectionConfig) -> None:
        """
        Establish a new connection to an endpoint.
        """
        ...

    @abstractmethod
    def is_connected_to(self, endpoint_name: str) -> bool:
        """
        Return whether this endpoint is connected to another endpoint with the given name.
        """
        ...

    @abstractmethod
    async def wait_until_connected_to(self, endpoint_name: str) -> None:
        """
        Block until connected to the named endpoint
        """
        ...

    @abstractmethod
    async def wait_until_connections_change(self) -> None:
        """
        Block until a change occurs in the remotes we are connected to.

        - new inbound or outbound connection
        - reverse connection
        """
        ...

    #
    # Subscriptions API
    #
    @abstractmethod
    def get_connected_endpoints_and_subscriptions(
        self
    ) -> Tuple[Tuple[Optional[str], Set[Type[BaseEvent]]], ...]:
        """
        Return all connected endpoints and their event type subscriptions to this endpoint.
        """
        ...

    @abstractmethod
    def is_remote_subscribed_to(
        self, remote_endpoint: str, event_type: Type[BaseEvent]
    ) -> bool:
        """
        Return ``True`` if the specified remote endpoint is subscribed to the specified event type
        from this endpoint. Otherwise return ``False``.
        """
        ...

    @abstractmethod
    def is_any_remote_subscribed_to(self, event_type: Type[BaseEvent]) -> bool:
        """
        Return ``True`` if at least one of the connected remote endpoints is subscribed to the
        specified event type from this endpoint. Otherwise return ``False``.
        """
        ...

    @abstractmethod
    def are_all_remotes_subscribed_to(self, event_type: Type[BaseEvent]) -> bool:
        """
        Return ``True`` if every connected remote endpoint is subscribed to the specified event
        type from this endpoint. Otherwise return ``False``.
        """
        ...

    @abstractmethod
    async def wait_until_remote_subscribed_to(
        self, remote_endpoint: str, event: Type[BaseEvent]
    ) -> None:
        """
        Block until the specified remote endpoint has subscribed to the specified event type
        from this endpoint.
        """
        ...

    @abstractmethod
    async def wait_until_any_remote_subscribed_to(self, event: Type[BaseEvent]) -> None:
        """
        Block until any other remote endpoint has subscribed to the specified event type
        from this endpoint.
        """
        ...

    async def wait_until_all_remotes_subscribed_to(
        self, event: Type[BaseEvent]
    ) -> None:
        """
        Block until all currently connected remote endpoints are subscribed to the specified
        event type from this endpoint.
        """
        ...

    #
    # Event API
    #
    @abstractmethod
    async def broadcast(
        self, item: BaseEvent, config: Optional[BroadcastConfig] = None
    ) -> None:
        """
        Broadcast an instance of :class:`~lahja.common.BaseEvent` on the event bus. Takes
        an optional second parameter of :class:`~lahja.common.BroadcastConfig` to decide
        where this event should be broadcasted to. By default, events are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        ...

    @abstractmethod
    def broadcast_nowait(
        self, item: BaseEvent, config: Optional[BroadcastConfig] = None
    ) -> None:
        """
        A sync compatible version of :meth:`~lahja.base.EndpointAPI.broadcast`

        .. warning::

            Heavy use of :meth:`~lahja.base.EndpointAPI.broadcast_nowait` in
            contiguous blocks of code without yielding to the `async`
            implementation should be expected to cause problems.

        """
        pass

    @abstractmethod
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
        ...

    @abstractmethod
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
        ...

    @abstractmethod
    async def stream(
        self, event_type: Type[TStreamEvent], num_events: Optional[int] = None
    ) -> AsyncGenerator[TStreamEvent, None]:
        """
        Stream all events that match the specified event type. This returns an
        ``AsyncIterable[BaseEvent]`` which can be consumed through an ``async for`` loop.
        An optional ``num_events`` parameter can be passed to stop streaming after a maximum amount
        of events was received.
        """
        yield  # type: ignore  # yield statemen convinces mypy this is a generator function

    @abstractmethod
    async def wait_for(self, event_type: Type[TWaitForEvent]) -> TWaitForEvent:
        """
        Wait for a single instance of an event that matches the specified event type.
        """
        ...


class BaseEndpoint(EndpointAPI):
    """
    Base class for endpoint implementations that implements shared/common logic
    """
    logger = logging.getLogger("lahja.endpoint.Endpoint")

    def __init__(self) -> None:
        try:
            # Trim the name to 20 characters in the case of overly long names
            # not causing us to take a performance hit constantly transporting it across the wire.
            self._request_id_base = self.name[:28].encode("ascii") + ":"
        except UnicodeDecodeError:
            raise Exception(
                f"TODO: Invalid endpoint name: '{self.name}'. Must be ASCII encodable string"
            )
        self._request_id_counter = itertools.count()

    #
    # Generation of request ids
    #
    def _get_request_id(self) -> RequestID:
        request_id = next(self._request_id_counter) % 65536
        return cast(RequestID, self._request_id_base + request_id.to_bytes(2, "little"))

    #
    # Common implementations
    #
    def __reduce__(self) -> None:  # type: ignore
        raise NotImplementedError("Endpoints cannot be pickled")

    async def connect_to_endpoints(self, *endpoints: ConnectionConfig) -> None:
        """
        Naive asynchronous implementation.  Subclasses should consider
        connecting concurrently.
        """
        for config in endpoints:
            await self.connect_to_endpoint(config)

    def is_connected_to(self, endpoint_name: str) -> bool:
        return any(
            name == endpoint_name
            for name, _
            in self.get_connected_endpoints_and_subscriptions()
        )

    async def wait_until_connected_to(self, endpoint_name: str) -> None:
        while True:
            if self.is_connected_to(endpoint_name):
                break
            await self.wait_until_connections_change()

    #
    # Event API
    #
    async def wait_for(self, event_type: Type[TWaitForEvent]) -> TWaitForEvent:
        """
        Wait for a single instance of an event that matches the specified event type.
        """
        agen = self.stream(event_type, num_events=1)
        event = await agen.asend(None)
        await agen.aclose()
        return event

    #
    # Subscription API
    #
    def is_remote_subscribed_to(
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

    def is_any_remote_subscribed_to(self, event_type: Type[BaseEvent]) -> bool:
        """
        Return ``True`` if at least one of the connected remote endpoints is subscribed to the
        specified event type from this endpoint. Otherwise return ``False``.
        """
        for endpoint, subscriptions in self.get_connected_endpoints_and_subscriptions():
            for subscribed_event_type in subscriptions:
                if subscribed_event_type is event_type:
                    return True

        return False

    def are_all_remotes_subscribed_to(self, event_type: Type[BaseEvent]) -> bool:
        """
        Return ``True`` if every connected remote endpoint is subscribed to the specified event
        type from this endpoint. Otherwise return ``False``.
        """
        for endpoint, subscriptions in self.get_connected_endpoints_and_subscriptions():
            if event_type not in subscriptions:
                return False

        return True

    async def wait_until_remote_subscribed_to(
        self, remote_endpoint: str, event: Type[BaseEvent]
    ) -> None:
        """
        Block until the specified remote endpoint has subscribed to the specified event type
        from this endpoint.
        """

        if self.is_remote_subscribed_to(remote_endpoint, event):
            return

        async for _ in self.stream(RemoteSubscriptionChanged):  # noqa: F841
            if self.is_remote_subscribed_to(remote_endpoint, event):
                return

    async def wait_until_any_remote_subscribed_to(self, event: Type[BaseEvent]) -> None:
        """
        Block until any other remote endpoint has subscribed to the specified event type
        from this endpoint.
        """

        if self.is_any_remote_subscribed_to(event):
            return

        async for _ in self.stream(RemoteSubscriptionChanged):  # noqa: F841
            if self.is_any_remote_subscribed_to(event):
                return

    async def wait_until_all_remotes_subscribed_to(
        self, event: Type[BaseEvent]
    ) -> None:
        """
        Block until all currently connected remote endpoints are subscribed to the specified
        event type from this endpoint.
        """

        if self.are_all_remotes_subscribed_to(event):
            return

        async for _ in self.stream(RemoteSubscriptionChanged):  # noqa: F841
            if self.are_all_remotes_subscribed_to(event):
                return

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
