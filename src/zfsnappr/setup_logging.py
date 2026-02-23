import logging
import sys
from collections.abc import Collection
from contextlib import contextmanager
from logging import Formatter, Logger, StreamHandler

_Level = int | str

ALL_LEVELS = tuple(logging.getLevelNamesMapping().values())


class ProtectedLogger(Logger):
    """A Logger whose level can only be set by calling `setLevel(..., force=True)`.
    Other attempts at setting the log level are simply no-op."""

    _level: int
    _allow_level_change: bool = False

    def __init__(self, name: str, level: int | str = 0) -> None:
        with self.allow_level_change():
            super().__init__(name, logging.NOTSET)

    @contextmanager
    def allow_level_change(self):
        try:
            self._allow_level_change = True
            yield
        finally:
            self._allow_level_change = False
    
    @property
    def level(self):
        return self._level

    @level.setter
    def level(self, level: int):  # type: ignore
        if self._allow_level_change:
            self._level = level

    def setLevel(self, level: int | str, force: bool = False):
        if force:
            with self.allow_level_change():
                super().setLevel(level)


def setup_logging(
    level: int = logging.INFO,
    level_others: int = logging.WARNING,
    loggers: Collection[str | tuple[str, _Level]] | None = None
) -> None:
    """
    Parameters
    ----------
    level
        The log level for loggers in `loggers` that are given by name only.

    level_others
        The log level for the root logger. Because all loggers have a level of `NOTSET` by default,
        this is also the default effective level for all loggers not in `loggers`.

    loggers
        Collection that may contain:
        - logger names. These loggers will be set to `level`.
        - tuple of logger name and log level. The loggers will be set to the specified level.

        The loggers of the current script and package are always contained in `loggers`.
    """
    loggers = set(loggers) if loggers is not None else set()
    # executed script and current package are always included
    loggers |= {'__main__', __name__.split('.')[0]}

    existing_loggers = {k for k, v in logging.root.manager.loggerDict.items() if isinstance(v, Logger)}
    logging.setLoggerClass(ProtectedLogger)

    # create all included loggers and set their log levels
    for item in loggers:
        if isinstance(item, str):
            set_level(logging.getLogger(item), level)
        else:
            set_level(logging.getLogger(item[0]), item[1])

    _setup_root_logger(level_others)

    if existing_loggers:
        log = logging.getLogger(__name__)
        log.debug(
            'Some loggers have already been created before the logger class could be changed.'
            ' This may be fixed by importing this module before any others.'
        )


def set_level(loggers: Logger | Collection[Logger], level: _Level):
    """Sets the log level of one or more loggers. Handles `ProtectedLogger` correctly."""
    if isinstance(loggers, Logger):
        loggers = [loggers]

    for logger in loggers:
        if isinstance(logger, ProtectedLogger):
            logger.setLevel(level, force=True)
        else:
            logger.setLevel(level)


def _setup_root_logger(loglevel: int):
    """Configures the root logger with formatter, handlers, and exception handling."""
    rootlog = logging.getLogger()
    rootlog.setLevel(loglevel)

    # configure formatter
    formatter = LeveledFormatter({
        ALL_LEVELS: IndentedFormatter('%(levelname)s: %(message)s', '%H:%M:%S'),
        logging.INFO: Formatter()
    })

    # configure stream handler
    stream_handler = StreamHandler()
    stream_handler.setFormatter(formatter)
    rootlog.addHandler(stream_handler)

    # configure level names
    logging.addLevelName(logging.DEBUG, 'DEBUG')
    logging.addLevelName(logging.INFO, 'INFO')
    logging.addLevelName(logging.WARNING, 'WARNING')
    logging.addLevelName(logging.ERROR, 'ERROR')
    logging.addLevelName(logging.CRITICAL, 'CRITICAL')

    # log uncaught errors
    # see https://stackoverflow.com/a/16993115
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            rootlog.info('Keyboard interrupt')
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        rootlog.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = handle_exception


class LeveledFormatter(Formatter):
    _formats: dict[int, Formatter]

    def __init__(self, formatters: dict[int | Collection[int], Formatter]) -> None:
        self._formats = dict()
        all_levels = set(logging.getLevelNamesMapping().values())
        for level, fmt in formatters.items():
            self.set_formatter(level, fmt)
        if diff := all_levels - self._formats.keys():
            name = logging.getLevelName(next(iter(diff)))
            raise ValueError(f"LeveledFormatter missing formatter for level: {name}")

    def set_formatter(self, level: int | Collection[int], formatter: Formatter):
        if isinstance(level, int):
            level = [level]
        for lv in level:
            self._formats[lv] = formatter

    def format(self, record):
        formatter = self._formats[record.levelno]
        return formatter.format(record)


class IndentedFormatter(Formatter):
    def format(self, record):
        orig_msg = record.msg
        if not isinstance(orig_msg, str):
            return super().format(record)

        stripped = orig_msg.lstrip(" ")
        num_spaces = len(orig_msg) - len(stripped)
        
        record.msg = stripped
        res = super().format(record)
        record.msg = orig_msg
        return " " * num_spaces + res
