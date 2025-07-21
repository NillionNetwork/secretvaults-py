"""
Structured logging configuration for SecretVaults.
"""

import os

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
    ]

    # Add console output for development
    if os.getenv("NODE_ENV") != "production":
        processors.append(structlog.dev.ConsoleRenderer())
    else:
        processors.append(structlog.processors.JSONRenderer())

    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


# Configure logging on module import
configure_logging()

# Create the main logger
Log = structlog.get_logger()


def set_log_level(level: str) -> None:
    """Set the log level."""
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if level.upper() not in valid_levels:
        Log.warning(f"Invalid log level: {level}. Ignoring.")
        return

    # This would need to be implemented based on your logging setup
    Log.info(f"Log level set to {level}")


def get_log_level() -> str:
    """Get the current log level."""
    # This would need to be implemented based on your logging setup
    return "INFO"


def clear_stored_log_level() -> None:
    """Clear any stored log level configuration."""
    # This would need to be implemented based on your logging setup
