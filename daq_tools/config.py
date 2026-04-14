import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from .utils import get_public_ip, configure_device_tracking

@dataclass
class InboundConfig:
    watch_dir: str | Path
    data_dir: str | Path = "data"  # base directory for queue + sinks
    file_pattern: str = "*.jsonl"


@dataclass
class ProcessingConfig:
    max_retries: int = 5
    backoff_base_seconds: float = 2.0


@dataclass
class SinkConfig:
    name: str
    type: str
    config: dict[str, Any] = field(default_factory=dict)

@dataclass
class DeviceConfig:
    add_public_ip: bool = False
    public_ip_tag_key: str = "public_ip"

@dataclass
class DAQConfig:
    inbound: InboundConfig
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    sinks: list[SinkConfig] = field(default_factory=list)
    device: DeviceConfig = field(default_factory=DeviceConfig)


def load_config(config_path: str | Path) -> DAQConfig:
    """Load configuration from a TOML file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "rb") as f:
        raw = tomllib.load(f)

    # Build nested dataclasses
    inbound = InboundConfig(**raw.get("inbound", {}))
    processing = ProcessingConfig(**raw.get("processing", {}))

    sinks = []
    for s in raw.get("sinks", []):
        sinks.append(SinkConfig(
            name=s["name"],
            type=s["type"],
            config=s.get("config", {})
        ))

    # === NEW DEVICE SECTION ===
    device_raw = raw.get("device", {})
    device = DeviceConfig(
        add_public_ip=device_raw.get("add_public_ip", False),
        public_ip_tag_key=device_raw.get("public_ip_tag_key", "public_ip"),
    )

    # Fetch public IP once at startup if enabled
    if device.add_public_ip:
        ip = get_public_ip()
        configure_device_tracking(
            add_public_ip=True,
            tag_key=device.public_ip_tag_key,
            ip=ip,
        )
    else:
        configure_device_tracking(add_public_ip=False)

    return DAQConfig(
        inbound=inbound,
        processing=processing,
        sinks=sinks,
        device=device,
    )


# Example config helper (for testing)
def create_example_config() -> DAQConfig:
    return DAQConfig(
        inbound=InboundConfig(watch_dir="/tmp/incoming"),
        sinks=[
            SinkConfig(name="local_file", type="file", config={"path": "/tmp/output.jsonl"}),
            # more sinks later
        ]
    )