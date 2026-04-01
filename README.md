# daq-tools

**Async JSONL file ingestor with fan-out to configurable data sinks.**

Designed for data acquisition (DAQ) systems. Watches a directory for new `.jsonl` files, queues them, and reliably distributes each file to multiple independent sinks (MQTT, local file, HTTP, etc.).

Built with modern Python 3.13+ async patterns (`asyncio.TaskGroup`, `watchfiles`, `aiofiles`, etc.).

## Features

- Simple single-class API — just drop it into your app
- Fan-out architecture: one file → multiple sinks
- Each sink is fully independent (own watcher, retry logic, error handling)
- Built-in retry with exponential backoff + `TryAgainError` for transient failures
- Graceful shutdown (finishes in-flight files)
- Configured entirely via TOML
- Currently supports:
  - Local file append (JSON or Line Protocol)
  - MQTT publishing (with measurement → topic mapping + fallback)

## Installation

### As a dependency (recommended)

In your project's `pyproject.toml`:

```toml
dependencies = [
    "daq-tools @ git+https://github.com/advect-technologies/daq-tools.git",
]
```
then run ```uv sync```

## Quick Start

```python
import asyncio
from daq_tools import DAQIngestor

async def main():
    async with DAQIngestor.from_config_file("config.toml") as ingestor:
        print("DAQIngestor running — drop .jsonl files into your watch_dir")
        await asyncio.sleep(3600)  # run for 1 hour (or integrate into your main loop)

if __name__ == "__main__":
    asyncio.run(main())
```


## Project Structure

* `daq_tools/` — main package
* `daq_tools/core.py` — DAQIngestor orchestrator
* `daq_tools/sinks/` — individual sink implementations
* `daq_tools/models.py` — DataPoint model (Line Protocol + JSON support)


Made with ❤️ by Advect Technologies