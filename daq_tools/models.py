import datetime as dt
import json
from dataclasses import dataclass, field, asdict, replace, InitVar
from typing import Any
from enum import StrEnum
from socket import gethostname

from .utils import (
    escape_lp_identifier,
    escape_lp_field_value,
    get_device_public_ip_tag,
)

class TimeRes(StrEnum):
    NS = 'ns'
    MUS = 'mus'
    MS = 'ms'
    S = 's'

@dataclass(kw_only=True)
class DataPoint:
    time: float | int = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc).timestamp())
    measurement: str
    tags: dict[str, str] = field(default_factory=dict)
    fields: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):

        if not (1_500_000_000 < self.time < 4_000_000_000):  # roughly 2017–2096
            raise ValueError(f"Invalid timestamp: {self.time} (expected seconds since epoch)")

        if not self.fields:
            raise ValueError("DataPoint must have at least one field")

        if self.measurement is None:
            raise ValueError('measurement must not be None')

        if 'id' not in self.tags:
            self.tags.update({'id':gethostname()})

        # Auto-add public IP tag (if enabled via config)
        ip, tag_key = get_device_public_ip_tag()
        if ip and tag_key not in self.tags:
            self.tags[tag_key] = ip

    def to_json(self) -> str:  
        return json.dumps(asdict(self))
    
    @classmethod
    def from_json(cls, payload: str) -> "DataPoint":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON payload: {e}") from e

        return cls(**data)    
    

    def to_line_protocol(self,ignore_errors=False,time_resolution=TimeRes.S) -> str:
        """Convert DataPoint to InfluxDB Line Protocol (ns precision default)."""
        try:
            if not self.fields:
                raise ValueError("Cannot write DataPoint with no fields")

            meas = escape_lp_identifier(self.measurement)

            tag_parts = [
                f"{escape_lp_identifier(k)}={escape_lp_identifier(v)}"
                for k, v in sorted(self.tags.items())   # sort → deterministic output
            ]
            tags_str = "," + ",".join(tag_parts) if tag_parts else ""

            field_parts = [
                f"{escape_lp_identifier(k)}={escape_lp_field_value(v)}"
                for k, v in self.fields.items()
                if v is not None
            ]
            fields_str = ",".join(field_parts)

            match time_resolution:

                case TimeRes.NS:
                    time_ = int(self.time * 1_000_000_000)
                
                case TimeRes.MUS:
                    time_ = int(self.time * 1_000_000)

                case TimeRes.MS:
                    time_ = int(self.time * 1_000)                    

                case TimeRes.S:
                    time_ = int(self.time)
                
                case _:
                    raise TypeError('Time Resolution must be ')

            return f"{meas}{tags_str} {fields_str} {time_}"
        except Exception as e:
            if ignore_errors: return None
            else:
                raise e
