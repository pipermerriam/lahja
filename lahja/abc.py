from abc import ABC, abstractmethod
from pathlib import Path
from typing import (  # noqa: F401
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Iterator,
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

from lahja.typing import RequestID


class SubscriptionAPI(ABC):
    @abstractmethod
    def unsubscribe(self) -> None:
        ...


class BroadcastConfigAPI:
    @abstractmethod
    def __init__(
        self,
        filter_endpoint: Optional[str] = None,
        filter_event_id: Optional[RequestID] = None,
        internal: bool = False,
    ) -> None:
        ...

    @abstractmethod
    def allowed_to_receive(self, endpoint: str) -> bool:
        ...


class EventAPI:
    is_bound = False

    @abstractmethod
    def get_origin(self) -> str:
        ...

    @abstractmethod
    def bind(self, endpoint: "EndpointAPI", id: Optional[RequestID]) -> None:
        ...

    @abstractmethod
    def broadcast_config(self, internal: bool = False) -> BroadcastConfigAPI:
        ...


TResponse = TypeVar("TResponse", bound=EventAPI)
TWaitForEvent = TypeVar("TWaitForEvent", bound=EventAPI)
TSubscribeEvent = TypeVar("TSubscribeEvent", bound=EventAPI)
TStreamEvent = TypeVar("TStreamEvent", bound=EventAPI)


class ConnectionAPI(ABC):
    @classmethod
    @abstractmethod
    async def connect_to(cls, path: Path) -> "ConnectionAPI":
        ...

    @abstractmethod
    async def close(self) -> None:
        ...

    @abstractmethod
    async def send_message(self, message: Msg) -> None:
        ...

    @abstractmethod
    async def read_message(self) -> Message:
        ...


class RemoteEndpointAPI(ABC):
    """
    Represents a connection to another endpoint.  Connections *can* be
    bi-directional with messages flowing in either direction.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    #
    # Object run lifecycle
    #
    @abstractmethod
    async def wait_started(self) -> None:
        ...

    @abstractmethod
    async def wait_ready(self) -> None:
        ...

    @abstractmethod
    async def wait_stopped(self) -> None:
        ...

    @property
    @abstractmethod
    def is_running(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_ready(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_stopped(self) -> bool:
        ...

    #
    # Running API
    #
    @abstractmethod
    def run(self) -> AsyncContextManager["RemoteEndpointAPI"]:
        """
        Context manager API for running endpoints.

        .. code-block:: python

            async with endpoint.run() as endpoint:
                ... # endpoint running within context
            ... # endpoint stopped after

        test
        """
        ...

    @abstractmethod
    async def stop(self) -> None:
        """
        Manually stop the remote endpoint.
        """
        ...

    #
    # Core external API
    #
    @abstractmethod
    def get_subscribed_events(self) -> Set[Type[EventAPI]]:
        """
        Return the event types this endpoint is current subscribed to.
        """
        ...

    @abstractmethod
    async def notify_subscriptions_updated(
        self, subscriptions: Set[Type[EventAPI]], block: bool = True
    ) -> None:
        """
        Alert the endpoint on the other side of this connection that the local
        subscriptions have changed. If ``block`` is ``True`` then this function
        will block until the remote endpoint has acknowledged the new
        subscription set. If ``block`` is ``False`` then this function will
        return immediately after the send finishes.
        """
        ...

    @abstractmethod
    async def send_message(self, message: Msg) -> None:
        ...

    @abstractmethod
    async def wait_until_subscription_initialized(self) -> None:
        """
        Block until at least one `SubscriptionsUpdated` event has been
        received.
        """
        ...


class EventBusAPI(ABC):
    #
    # Event API
    #
    @abstractmethod
    async def broadcast(
        self, item: EventAPI, config: Optional[BroadcastConfigAPI] = None
    ) -> None:
        """
        Broadcast an instance of :class:`~lahja.abc.EventAPI` on the event bus. Takes
        an optional second parameter of :class:`~lahja.common.BroadcastConfig` to decide
        where this event should be broadcasted to. By default, events are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        ...

    @abstractmethod
    def broadcast_nowait(
        self, item: EventAPI, config: Optional[BroadcastConfigAPI] = None
    ) -> None:
        """
        A sync compatible version of :meth:`~lahja.base.EndpointAPI.broadcast`

        .. warning::

            Heavy use of :meth:`~lahja.base.EndpointAPI.broadcast_nowait` in
            contiguous blocks of code without yielding to the `async`
            implementation should be expected to cause problems.

        """
        ...

    @abstractmethod
    async def request(
        self,
        item: BaseRequestResponseEvent[TResponse],
        config: Optional[BroadcastConfigAPI] = None,
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
    def subscribe(
        self,
        event_type: Type[TSubscribeEvent],
        handler: Callable[[TSubscribeEvent], Union[Any, Awaitable[Any]]],
    ) -> SubscriptionAPI:
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
    # Running API
    #
    @abstractmethod
    def run(self) -> AsyncContextManager["EndpointAPI"]:
        """
        Context manager API for running endpoints.

        .. code-block:: python

            async with endpoint.run() as endpoint:
                ... # endpoint running within context
            ... # endpoint stopped after

        """
        ...

    #
    # Serving API
    #
    @classmethod
    @abstractmethod
    def serve(cls, config: ConnectionConfig) -> AsyncContextManager["EndpointAPI"]:
        """
        Context manager API for running and endpoint server.

        .. code-block:: python

            async with EndpointClass.serve(config):
                ... # server running within context
            ... # server stopped

        """
        ...

    #
    # Connection API
    #

    @abstractmethod
    async def connect_to_endpoints(self, *endpoints: ConnectionConfig) -> None:
        """
        Establish connections to the given endpoints.
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
        Return once a connection exists to an endpoint with the given name.
        """
        ...

    @abstractmethod
    def get_connected_endpoints_and_subscriptions(
        self
    ) -> Tuple[Tuple[str, Set[Type[EventAPI]]], ...]:
        """
        Return 2-tuples for all all connected endpoints containing the name of
        the endpoint coupled with the set of messages the endpoint subscribes
        to
        """
        ...

    @abstractmethod
    async def wait_until_connections_change(self) -> None:
        """
        Block until the set of connected remote endpoints changes.
        """
        ...

    #
    # Subscription API
    #
    @abstractmethod
    def get_subscribed_events(self) -> Set[Type[EventAPI]]:
        """
        Return the set of event types this endpoint subscribes to.
        """
        ...

    @abstractmethod
    async def wait_until_endpoint_subscriptions_change(self) -> None:
        """
        Block until any subscription change occurs on any remote endpoint or
        the set of remote endpoints changes
        """
        ...

    @abstractmethod
    def is_endpoint_subscribed_to(
        self, remote_endpoint: str, event_type: Type[EventAPI]
    ) -> bool:
        """
        Return ``True`` if the specified remote endpoint is subscribed to the specified event type
        from this endpoint. Otherwise return ``False``.
        """
        ...

    @abstractmethod
    def is_any_endpoint_subscribed_to(self, event_type: Type[EventAPI]) -> bool:
        """
        Return ``True`` if at least one of the connected remote endpoints is subscribed to the
        specified event type from this endpoint. Otherwise return ``False``.
        """
        ...

    @abstractmethod
    def are_all_endpoints_subscribed_to(self, event_type: Type[EventAPI]) -> bool:
        """
        Return ``True`` if every connected remote endpoint is subscribed to the specified event
        type from this endpoint. Otherwise return ``False``.
        """
        ...

    @abstractmethod
    async def wait_until_endpoint_subscribed_to(
        self, remote_endpoint: str, event: Type[EventAPI]
    ) -> None:
        """
        Block until the specified remote endpoint has subscribed to the specified event type
        from this endpoint.
        """
        ...

    @abstractmethod
    async def wait_until_any_endpoint_subscribed_to(
        self, event: Type[EventAPI]
    ) -> None:
        """
        Block until any other remote endpoint has subscribed to the specified event type
        from this endpoint.
        """
        ...

    @abstractmethod
    async def wait_until_all_endpoints_subscribed_to(
        self, event: Type[EventAPI], *, include_self: bool = True
    ) -> None:
        """
        Block until all currently connected remote endpoints are subscribed to the specified
        event type from this endpoint.
        """
        ...
