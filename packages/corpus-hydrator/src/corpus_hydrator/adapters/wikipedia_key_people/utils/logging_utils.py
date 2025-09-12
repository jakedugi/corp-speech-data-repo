"""
Structured Logging Utilities for Wikipedia Key People Scraper

This module provides structured JSON logging and metrics collection
for observability and debugging.
"""

import logging
import json
import time
import threading
from typing import Dict, Any, Optional
from datetime import datetime
from collections import defaultdict
import sys


class StructuredLogger:
    """
    Structured JSON logger with metrics collection.

    Provides both human-readable and machine-readable logging output.
    """

    def __init__(self, name: str, level: int = logging.INFO):
        """Initialize the structured logger."""
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Remove existing handlers to avoid duplicates
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        # Add our structured handler
        handler = StructuredLogHandler()
        handler.setLevel(level)
        self.logger.addHandler(handler)

        # Metrics collection
        self.metrics = defaultdict(int)
        self.timers = {}
        self.counters = defaultdict(lambda: defaultdict(int))

    def log_event(self, event: str, **kwargs):
        """Log a structured event."""
        log_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'logger': self.name,
            'event': event,
            'level': 'INFO',
            **kwargs
        }
        self.logger.info(json.dumps(log_data, default=str))

    def log_error(self, event: str, error: Exception = None, **kwargs):
        """Log a structured error event."""
        log_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'logger': self.name,
            'event': event,
            'level': 'ERROR',
            **kwargs
        }

        if error:
            log_data['error_type'] = type(error).__name__
            log_data['error_message'] = str(error)

        self.logger.error(json.dumps(log_data, default=str))

    def start_timer(self, name: str):
        """Start a timer for performance measurement."""
        self.timers[name] = time.time()
        self.log_event('timer_started', timer_name=name)

    def end_timer(self, name: str) -> float:
        """End a timer and return elapsed time."""
        if name not in self.timers:
            self.log_error('timer_not_found', timer_name=name)
            return 0.0

        elapsed = time.time() - self.timers[name]
        del self.timers[name]

        self.log_event('timer_ended', timer_name=name, elapsed_seconds=elapsed)
        return elapsed

    def increment_counter(self, category: str, name: str, value: int = 1):
        """Increment a counter."""
        self.counters[category][name] += value
        self.log_event('counter_incremented',
                      category=category,
                      counter_name=name,
                      value=value,
                      total=self.counters[category][name])

    def log_metrics(self):
        """Log current metrics snapshot."""
        metrics_data = {
            'counters': dict(self.counters),
            'active_timers': list(self.timers.keys()),
            'timestamp': datetime.utcnow().isoformat()
        }
        self.log_event('metrics_snapshot', **metrics_data)

    def log_company_processing(self, company_name: str, ticker: str, status: str, **kwargs):
        """Log company processing event."""
        self.log_event('company_processing',
                      company_name=company_name,
                      ticker=ticker,
                      status=status,
                      **kwargs)

    def log_http_request(self, url: str, method: str = 'GET', status_code: Optional[int] = None,
                        response_time: Optional[float] = None, cached: bool = False, **kwargs):
        """Log HTTP request event."""
        self.log_event('http_request',
                      url=url,
                      method=method,
                      status_code=status_code,
                      response_time=response_time,
                      cached=cached,
                      **kwargs)


class StructuredLogHandler(logging.Handler):
    """Custom logging handler for structured JSON output."""

    def __init__(self, stream=None):
        """Initialize the handler."""
        super().__init__()
        self.stream = stream or sys.stderr

    def emit(self, record):
        """Emit a log record."""
        try:
            # Try to parse as JSON first (for structured logs)
            if hasattr(record, 'getMessage'):
                message = record.getMessage()
                try:
                    # If it's valid JSON, pretty print it
                    parsed = json.loads(message)
                    formatted = json.dumps(parsed, indent=2, default=str)
                except (json.JSONDecodeError, TypeError):
                    # Not JSON, format as regular log
                    formatted = self.format(record)
            else:
                formatted = self.format(record)

            self.stream.write(formatted + '\n')
            self.stream.flush()

        except Exception:
            self.handleError(record)


class MetricsCollector:
    """
    Thread-safe metrics collection for the scraper.

    Tracks various performance and quality metrics.
    """

    def __init__(self):
        """Initialize metrics collector."""
        self._metrics = defaultdict(lambda: defaultdict(int))
        self._timers = {}
        self._lock = threading.Lock()

    def increment(self, category: str, metric: str, value: int = 1):
        """Increment a metric counter."""
        with self._lock:
            self._metrics[category][metric] += value

    def set_gauge(self, category: str, metric: str, value: float):
        """Set a gauge metric."""
        with self._lock:
            self._metrics[category][metric] = value

    def start_timer(self, name: str):
        """Start a timer."""
        with self._lock:
            self._timers[name] = time.time()

    def end_timer(self, name: str) -> float:
        """End a timer and return elapsed time."""
        with self._lock:
            if name in self._timers:
                elapsed = time.time() - self._timers[name]
                del self._timers[name]
                return elapsed
            return 0.0

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics snapshot."""
        with self._lock:
            return {
                'metrics': dict(self._metrics),
                'active_timers': list(self._timers.keys()),
                'timestamp': datetime.utcnow().isoformat()
            }

    def reset(self):
        """Reset all metrics."""
        with self._lock:
            self._metrics.clear()
            self._timers.clear()


# Global instances
structured_logger = StructuredLogger('wikipedia_key_people')
metrics_collector = MetricsCollector()


def setup_structured_logging(level: str = 'INFO', json_only: bool = False):
    """
    Set up structured logging for the application.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        json_only: If True, only output JSON logs (no human-readable)
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    if json_only:
        # JSON-only output
        handler = StructuredLogHandler()
    else:
        # Human-readable with structured data
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)

    handler.setLevel(numeric_level)
    root_logger.addHandler(handler)

    # Set up our structured logger
    structured_logger.logger.setLevel(numeric_level)
