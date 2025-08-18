"""
Structured logging configuration for SecretVaults.
"""

import logging
import structlog


def configure_logging() -> None:
    """Configure structured logging."""
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ]

    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Set a handler and a default level
    logging.basicConfig(level=logging.ERROR)


# Configure logging on module import
configure_logging()
Log = structlog.get_logger()


def set_log_level(level: str) -> None:
    """Set the structlog/stdlib log level."""
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if level.upper() not in valid_levels:
        Log.warning(f"Invalid log level: {level}. Ignoring.")
        return

    logging.getLogger().setLevel(getattr(logging, level.upper()))
    Log.info(f"Log level set to {level}")


def get_log_level() -> str:
    """Get the current structlog/stdlib log level."""
    level = logging.getLogger().getEffectiveLevel()
    return logging.getLevelName(level)


def clear_stored_log_level() -> None:
    """Reset to default INFO level."""
    logging.getLogger().setLevel(logging.ERROR)
    Log.info("Log level reset to INFO")
