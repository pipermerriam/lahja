import asyncio
import itertools
import logging
import multiprocessing
from multiprocessing.synchronize import (
    Event,
)
import time
from typing import (  # noqa: F401
    List,
    NamedTuple,
    Optional,
    Tuple,
)

from lahja import (
    BroadcastConfig,
    ConnectionConfig,
    Endpoint,
)
from lahja.tools.benchmark.constants import (
    DRIVER_ENDPOINT,
    REPORTER_ENDPOINT,
    ROOT_ENDPOINT,
)
from lahja.tools.benchmark.stats import (
    GlobalStatistic,
    LocalStatistic,
)
from lahja.tools.benchmark.typing import (
    PerfMeasureEvent,
    RawMeasureEntry,
    ShutdownEvent,
    TotalRecordedEvent,
)
from lahja.tools.benchmark.utils.reporting import (
    print_full_report,
)


class DriverProcessConfig(NamedTuple):
    num_events: int
    connected_endpoints: Tuple[ConnectionConfig, ...]
    throttle: float
    payload_bytes: int
    ready_events: List[Event]


class DriverProcess:

    def __init__(self, config: DriverProcessConfig) -> None:
        self._config = config
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch,
            args=(self._config,)
        )
        self._process.start()

    @staticmethod
    def launch(config: DriverProcessConfig) -> None:
        # UNCOMMENT FOR DEBUGGING
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.INFO)

        # wait until the reporter and all the consumers are ready
        for event in config.ready_events:
            event.wait()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(DriverProcess.worker(config))

    @staticmethod
    async def worker(config: DriverProcessConfig) -> None:
        with Endpoint() as event_bus:
            await event_bus.start_serving(ConnectionConfig.from_name(DRIVER_ENDPOINT))
            await event_bus.connect_to_endpoints(*config.connected_endpoints)

            payload = b'\x00' * config.payload_bytes
            for n in range(config.num_events):
                await asyncio.sleep(config.throttle)
                await event_bus.broadcast(
                    PerfMeasureEvent(payload, n, time.time())
                )


class ConsumerProcess:

    def __init__(self, name: str, num_events: int, ready_event: Event) -> None:
        self._name = name
        self._num_events = num_events
        self._process: Optional[multiprocessing.Process] = None
        self._ready_event = ready_event

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch,
            args=(self._name, self._num_events, self._ready_event)
        )
        self._process.start()

    @staticmethod
    async def worker(name: str, num_events: int, ready_event: Event) -> None:
        with Endpoint() as event_bus:
            await event_bus.start_serving(ConnectionConfig.from_name(name))
            await event_bus.connect_to_endpoints(ConnectionConfig.from_name(REPORTER_ENDPOINT))

            ready_event.set()

            counter = itertools.count(1)
            stats = LocalStatistic()
            async for event in event_bus.stream(PerfMeasureEvent):
                stats.add(RawMeasureEntry(
                    sent_at=event.sent_at,
                    received_at=time.time()
                ))

                if next(counter) == num_events:
                    await event_bus.broadcast(
                        TotalRecordedEvent(stats.crunch(event_bus.name)),
                        BroadcastConfig(filter_endpoint=REPORTER_ENDPOINT)
                    )
                    break

    @staticmethod
    def launch(name: str, num_events: int, ready_event: Event) -> None:
        # UNCOMMENT FOR DEBUGGING
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.INFO)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(ConsumerProcess.worker(name, num_events, ready_event))


class ReportingProcessConfig(NamedTuple):
    num_processes: int
    num_events: int
    throttle: float
    payload_bytes: int
    ready_event: Event


class ReportingProcess:

    def __init__(self, config: ReportingProcessConfig) -> None:
        self._name = REPORTER_ENDPOINT
        self._config = config
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch,
            args=(self._config,)
        )
        self._process.start()

    @staticmethod
    async def worker(logger: logging.Logger,
                     config: ReportingProcessConfig) -> None:
        with Endpoint() as event_bus:
            await event_bus.start_serving(ConnectionConfig.from_name(REPORTER_ENDPOINT))
            await event_bus.connect_to_endpoints(
                ConnectionConfig.from_name(ROOT_ENDPOINT),
            )

            config.ready_event.set()

            global_statistic = GlobalStatistic()
            async for event in event_bus.stream(TotalRecordedEvent):

                global_statistic.add(event.total)
                if len(global_statistic) == config.num_processes:
                    print_full_report(
                        logger,
                        config.num_processes,
                        config.num_events,
                        global_statistic
                    )
                    await event_bus.broadcast(
                        ShutdownEvent(),
                        BroadcastConfig(filter_endpoint=ROOT_ENDPOINT)
                    )
                    break

    @staticmethod
    def launch(config: ReportingProcessConfig) -> None:
        logging.basicConfig(level=logging.INFO, format='%(message)s')
        logger = logging.getLogger('reporting')

        loop = asyncio.get_event_loop()
        loop.run_until_complete(ReportingProcess.worker(logger, config))
