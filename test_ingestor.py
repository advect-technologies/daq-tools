#!/usr/bin/env python3
"""
Simple test script for DAQIngestor.

Usage:
    python test_ingestor.py
"""

import asyncio
import logging
import sys
from pathlib import Path

from daq_tools.core import DAQIngestor
from daq_tools.config import load_config

# Windows asyncio fix — must be very early
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


async def main() -> None:
    config_path = Path("config.toml")

    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        logger.info("Please create a config.toml file (see config.toml.example below)")
        sys.exit(1)

    try:
        ingestor = DAQIngestor.from_config_file(config_path)
        logger.info(f"Loaded config from {config_path}")
        logger.info(f"Watch directory: {ingestor.watch_dir}")
        logger.info(f"Data directory:  {ingestor.base_data_dir}")
        logger.info(f"Number of sinks: {len(ingestor.config.sinks)}")
        
        async with ingestor:
            logger.info("=" * 60)
            logger.info("DAQIngestor is now RUNNING")
            logger.info("→ Drop .jsonl files into your watch_dir to test")
            logger.info("→ Press Ctrl+C to stop gracefully")
            logger.info("=" * 60)

            # Keep running until interrupted
            try:
                while True:
                    await asyncio.sleep(10)
                    # Optional: show status every 10 seconds
                    active_sinks = len([s for s in ingestor.sinks if s._running])
                    logger.debug(f"Status: {active_sinks}/{len(ingestor.sinks)} sinks active")
            except asyncio.CancelledError:
                pass

    except KeyboardInterrupt:
        logger.info("Received Ctrl+C, shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Test script stopped by user.")