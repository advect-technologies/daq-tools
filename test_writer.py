#!/usr/bin/env python3
import asyncio
import json
import random
import time
import tomllib
from pathlib import Path

import aiofiles


async def write_sample_data(
    watch_dir: str | Path, num_files: int = 3, points_per_file: int = 10
):
    watch_dir = Path(watch_dir)
    watch_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating realistic test data → {watch_dir}")

    for i in range(num_files):
        filename = f"test_batch_{i:03d}_{int(time.time())}.jsonl"
        filepath = watch_dir / filename

        async with aiofiles.open(filepath, "w", encoding="utf-8") as f:
            for j in range(points_per_file):
                point = {
                    "measurement": "daq_test",
                    "fields": {
                        "temperature_c": round(random.uniform(18.0, 35.0), 2),
                        "pressure_hpa": round(random.uniform(990.0, 1025.0), 1),
                        "humidity_pct": random.randint(30, 85),
                        "voltage": round(random.uniform(3.1, 3.4), 3),
                        "status_code": random.randint(0, 5),  # Int field
                        "heater_on": random.choice([True, False]),  # Bool field
                        "nan_test": float("nan"),  # should be dropped
                        "empty_str_test": "",  # should be dropped
                        "none_test": None,  # should be dropped
                    },
                    "tags": {
                        "location": random.choice(["rig_a", "rig_b", "test_bench"]),
                        "sensor_id": f"s{i:02d}",
                        "batch": str(i),
                    },
                    "time": time.time(),
                }
                await f.write(json.dumps(point) + "\n")

        print(f"  Wrote {points_per_file} points → {filename}")
        await asyncio.sleep(1)  # slight delay between files

    print(f"\n✅ Done! Generated {num_files} files with realistic data.")


def load_watch_dir(config_path: str = "config.toml") -> Path:
    """Extract watch_dir from your config.toml"""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")

    with open(path, "rb") as f:
        config = tomllib.load(f)

    watch_dir = config.get("inbound", {}).get("watch_dir")
    if not watch_dir:
        raise ValueError("watch_dir not found in config.toml")

    return Path(watch_dir)


if __name__ == "__main__":
    try:
        watch_dir = load_watch_dir()
        print(f"Using watch_dir from config: {watch_dir}")
    except Exception as e:
        print(f"Could not load from config: {e}")
        watch_dir = input("Enter watch_dir path manually: ").strip()

    asyncio.run(write_sample_data(watch_dir))
