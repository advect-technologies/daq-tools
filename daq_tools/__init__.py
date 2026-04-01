"""
DAQ Tools - Async JSONL file ingestor with fan-out to configurable sinks.
"""

from .config import DAQConfig, load_config
from .core import DAQIngestor
from .sinks.base import AsyncSink, TryAgainError
from .sinks.file import FileSink
from .sinks.mqtt import MqttSink
from .sinks.registry import create_sink, create_all_sinks, register_sink_type

__version__ = "0.1.0"
__all__ = [
    "DAQIngestor",
    "DAQConfig",
    "load_config",
    "AsyncSink",
    "TryAgainError",
    "FileSink",
    "MqttSink",
    "create_sink",
    "create_all_sinks",
    "register_sink_type",
]