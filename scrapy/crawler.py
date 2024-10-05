from __future__ import annotations

import logging
import pprint
import signal
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
)

from twisted.internet.defer import (
    Deferred,
    DeferredList,
    inlineCallbacks,
    maybeDeferred,
)
from zope.interface.verify import verifyClass

from scrapy import Spider, signals
from scrapy.addons import AddonManager
from scrapy.core.engine import ExecutionEngine
from scrapy.exceptions import ScrapyDeprecationWarning
from scrapy.extension import ExtensionManager
from scrapy.interfaces import ISpiderLoader
from scrapy.logformatter import LogFormatter
from scrapy.settings import BaseSettings, Settings, overridden_settings
from scrapy.signalmanager import SignalManager
from scrapy.statscollectors import StatsCollector
from scrapy.utils.log import (
    LogCounterHandler,
    configure_logging,
    get_scrapy_root_handler,
    install_scrapy_root_handler,
    log_reactor_info,
    log_scrapy_info,
)
from scrapy.utils.misc import build_from_crawler, load_object
from scrapy.utils.ossignal import install_shutdown_handlers, signal_names
from scrapy.utils.reactor import (
    install_reactor,
    is_asyncio_reactor_installed,
    verify_installed_asyncio_event_loop,
    verify_installed_reactor,
)

if TYPE_CHECKING:
    from scrapy.utils.request import RequestFingerprinter


logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class Crawler:
    def __init__(
        self,
        spidercls: Type[Spider],
        settings: Union[None, Dict[str, Any], Settings] = None,
        init_reactor: bool = False,
    ):
        if isinstance(spidercls, Spider):
            raise ValueError("The spidercls argument must be a class, not an object")

        if isinstance(settings, dict) or settings is None:
            settings = Settings(settings)

        self.spidercls: Type[Spider] = spidercls
        self.settings: Settings = settings.copy()
        self.spidercls.update_settings(self.settings)
        self._update_root_log_handler()

        self.addons: AddonManager = AddonManager(self)
        self.signals: SignalManager = SignalManager(self)

        self._init_reactor: bool = init_reactor
        self.crawling: bool = False
        self._started: bool = False

        self.extensions: Optional[ExtensionManager] = None
        self.stats: Optional[StatsCollector] = None
        self.logformatter: Optional[LogFormatter] = None
        self.request_fingerprinter: Optional[RequestFingerprinter] = None
        self.spider: Optional[Spider] = None
        self.engine: Optional[ExecutionEngine] = None

    def _update_root_log_handler(self) -> None:
        """Update root logging handler with new settings."""
        if get_scrapy_root_handler() is not None:
            install_scrapy_root_handler(self.settings)

    def _apply_settings(self) -> None:
        """Apply and freeze the settings for the crawler."""
        if self.settings.frozen:
            return

        self.addons.load_settings(self.settings)
        self.stats = load_object(self.settings["STATS_CLASS"])(self)

        handler = LogCounterHandler(self, level=self.settings.get("LOG_LEVEL"))
        logging.root.addHandler(handler)

        self.__remove_handler = lambda: logging.root.removeHandler(handler)
        self.signals.connect(self.__remove_handler, signals.engine_stopped)

        lf_cls: Type[LogFormatter] = load_object(self.settings["LOG_FORMATTER"])
        self.logformatter = lf_cls.from_crawler(self)

        self.request_fingerprinter = build_from_crawler(
            load_object(self.settings["REQUEST_FINGERPRINTER_CLASS"]),
            self,
        )

        reactor_class: str = self.settings["TWISTED_REACTOR"]
        event_loop: str = self.settings["ASYNCIO_EVENT_LOOP"]

        if self._init_reactor:
            if reactor_class:
                install_reactor(reactor_class, event_loop)
            else:
                from twisted.internet import reactor  # noqa: F401
            log_reactor_info()

        if reactor_class:
            verify_installed_reactor(reactor_class)
            if is_asyncio_reactor_installed() and event_loop:
                verify_installed_asyncio_event_loop(event_loop)

        self.extensions = ExtensionManager.from_crawler(self)
        self.settings.freeze()

        overridden = dict(overridden_settings(self.settings))
        logger.info("Overridden settings:\n%(settings)s", {"settings": pprint.pformat(overridden)})

    @inlineCallbacks
    def crawl(self, *args: Any, **kwargs: Any) -> Generator[Deferred[Any], Any, None]:
        """Start the crawling process."""
        if self.crawling:
            raise RuntimeError("Crawling already taking place")
        if self._started:
            warnings.warn(
                "Running Crawler.crawl() more than once is deprecated.",
                ScrapyDeprecationWarning,
                stacklevel=2,
            )
        self.crawling = self._started = True

        try:
            self.spider = self._create_spider(*args, **kwargs)
            self._apply_settings()
            self._update_root_log_handler()
            self.engine = self._create_engine()
            start_requests = iter(self.spider.start_requests())
            yield self.engine.open_spider(self.spider, start_requests)
            yield maybeDeferred(self.engine.start)
        except Exception:
            self.crawling = False
            if self.engine is not None:
                yield self.engine.close()
            raise

    def _create_spider(self, *args: Any, **kwargs: Any) -> Spider:
        """Create a new spider instance."""
        return self.spidercls.from_crawler(self, *args, **kwargs)

    def _create_engine(self) -> ExecutionEngine:
        """Create the execution engine for the crawler."""
        return ExecutionEngine(self, lambda _: self.stop())

    @inlineCallbacks
    def stop(self) -> Generator[Deferred[Any], Any, None]:
        """Gracefully stop the crawling process."""
        if self.crawling:
            self.crawling = False
            assert self.engine
            yield maybeDeferred(self.engine.stop)

    @staticmethod
    def _get_component(component_class, components):
        """Return the component if found in the list of components."""
        for component in components:
            if isinstance(component, component_class):
                return component
        return None

    def get_addon(self, cls):
        """Get the specific addon component."""
        return self._get_component(cls, self.addons.addons)

    def get_downloader_middleware(self, cls):
        """Get the downloader middleware."""
        if not self.engine:
            raise RuntimeError(
                "Crawler.get_downloader_middleware() can only be called after "
                "the crawl engine has been created."
            )
        return self._get_component(cls, self.engine.downloader.middleware.middlewares)

    def get_extension(self, cls):
        """Get the extension."""
        if not self.extensions:
            raise RuntimeError(
                "Crawler.get_extension() can only be called after the "
                "extension manager has been created."
            )
        return self._get_component(cls, self.extensions.middlewares)

    def get_item_pipeline(self, cls):
        """Get the item pipeline."""
        if not self.engine:
            raise RuntimeError(
                "Crawler.get_item_pipeline() can only be called after the "
                "crawl engine has been created."
            )
        return self._get_component(cls, self.engine.scraper.itemproc.middlewares)

    def get_spider_middleware(self, cls):
        """Get the spider middleware."""
        if not self.engine:
            raise RuntimeError(
                "Crawler.get_spider_middleware() can only be called after the "
                "crawl engine has been created."
            )
        return self._get_component(cls, self.engine.scraper.spidermw.middlewares)


class CrawlerRunner:
    """
    A helper class that keeps track of, manages, and runs crawlers inside an already setup Twisted reactor.
    """

    crawlers = property(
        lambda self: self._crawlers,
        doc="Set of :class:`crawlers <scrapy.crawler.Crawler>` started by "
        ":meth:`crawl` and managed by this class.",
    )

    @staticmethod
    def _get_spider_loader(settings: BaseSettings):
        """Get SpiderLoader instance from settings"""
        cls_path = settings.get("SPIDER_LOADER_CLASS")
        loader_cls = load_object(cls_path)
        verifyClass(ISpiderLoader, loader_cls)
        return loader_cls.from_settings(settings.frozencopy())

    def __init__(self, settings: Union[Dict[str, Any], Settings, None] = None):
        if isinstance(settings, dict) or settings is None:
            settings = Settings(settings)
        self.settings = settings
        self.spider_loader = self._get_spider_loader(settings)
        self._crawlers: Set[Crawler] = set()
        self._active: Set[Deferred[None]] = set()
        self.bootstrap_failed = False

    def crawl(
        self,
        crawler_or_spidercls: Union[Type[Spider], str, Crawler],
        *args: Any,
        **kwargs: Any,
    ) -> Deferred[None]:
        """Run a crawler with the provided arguments."""
        if isinstance(crawler_or_spidercls, Spider):
            raise ValueError(
                "The crawler_or_spidercls argument cannot be a spider object, "
                "it must be a spider class (or a Crawler object)"
            )
        crawler = self.create_crawler(crawler_or_spidercls)
        return self._crawl(crawler, *args, **kwargs)

    def _crawl(self, crawler: Crawler, *args: Any, **kwargs: Any) -> Deferred[None]:
        self.crawlers.add(crawler)
        d = crawler.crawl(*args, **kwargs)
        self._active.add(d)

        def _done(result: _T) -> _T:
            self.crawlers.discard(crawler)
            self._active.discard(d)
            self.bootstrap_failed |= not getattr(crawler, "spider", None)
            return result

        return d.addBoth(_done)

    def create_crawler(
        self, crawler_or_spidercls: Union[Type[Spider], str, Crawler]
    ) -> Crawler:
        """Return a Crawler object."""
        if isinstance(crawler_or_spidercls, Spider):
            raise ValueError(
                "The crawler_or_spidercls argument cannot be a spider object, "
                "it must be a spider class (or a Crawler object)"
            )
        if isinstance(crawler_or_spidercls, Crawler):
            return crawler_or_spidercls
        return self._create_crawler(crawler_or_spidercls)

    def _create_crawler(self, spidercls: Union[str, Type[Spider]]) -> Crawler:
        if isinstance(spidercls, str):
            spidercls = self.spider_loader.load(spidercls)
        return Crawler(cast(Type[Spider], spidercls), self.settings)

    def stop(self) -> Deferred[Any]:
        """Stop all the crawling jobs taking place."""
        return DeferredList([c.stop() for c in list(self.crawlers)])

    @inlineCallbacks
    def join(self) -> Generator[Deferred[Any], Any, None]:
        """Join all active crawlers."""
        while self._active:
            yield DeferredList(self._active)


class CrawlerProcess(CrawlerRunner):
    """
    A class to run multiple scrapy crawlers in a process simultaneously.
    """

    def __init__(
        self,
        settings: Union[Dict[str, Any], Settings, None] = None,
        install_root_handler: bool = True,
    ):
        super().__init__(settings)
        configure_logging(self.settings, install_root_handler)
        log_scrapy_info(self.settings)
        self._initialized_reactor = False

    def _signal_shutdown(self, signum: int, _: Any) -> None:
        from twisted.internet import reactor

        install_shutdown_handlers(self._signal_kill)
        signame = signal_names[signum]
        logger.info(
            "Received %(signame)s, shutting down gracefully. Send again to force ",
            {"signame": signame},
        )
        reactor.callFromThread(self._graceful_stop_reactor)

    def _signal_kill(self, signum: int, _: Any) -> None:
        from twisted.internet import reactor

        install_shutdown_handlers(signal.SIG_IGN)
        signame = signal_names[signum]
        logger.info(
            "Received %(signame)s twice, forcing unclean shutdown", {"signame": signame}
        )
        reactor.callFromThread(self._stop_reactor)

    def _create_crawler(self, spidercls: Union[Type[Spider], str]) -> Crawler:
        if isinstance(spidercls, str):
            spidercls = self.spider_loader.load(spidercls)
        init_reactor = not self._initialized_reactor
        self._initialized_reactor = True
        return Crawler(
            cast(Type[Spider], spidercls), self.settings, init_reactor=init_reactor
        )

    def start(
        self, stop_after_crawl: bool = True, install_signal_handlers: bool = True
    ) -> None:
        """Start the reactor and begin the crawling process."""
        from twisted.internet import reactor

        if stop_after_crawl:
            d = self.join()
            if d.called:
                return
            d.addBoth(self._stop_reactor)

        resolver_class = load_object(self.settings["DNS_RESOLVER"])
        resolver = build_from_crawler(resolver_class, self, reactor=reactor)
        resolver.install_on_reactor()

        tp = reactor.getThreadPool()
        tp.adjustPoolsize(maxthreads=self.settings.getint("REACTOR_THREADPOOL_MAXSIZE"))
        reactor.addSystemEventTrigger("before", "shutdown", self.stop)

        if install_signal_handlers:
            reactor.addSystemEventTrigger(
                "after", "startup", install_shutdown_handlers, self._signal_shutdown
            )

        reactor.run(installSignalHandlers=install_signal_handlers)

    def _graceful_stop_reactor(self) -> Deferred[Any]:
        """Gracefully stop the reactor."""
        d = self.stop()
        d.addBoth(self._stop_reactor)
        return d

    def _stop_reactor(self, _: Any = None) -> None:
        """Stop the reactor."""
        from twisted.internet import reactor

        try:
            reactor.stop()
        except RuntimeError:
            pass
