import urllib.request
import json
from typing import Optional, Any

# Module-level cache for device public IP tracking
_DEVICE_PUBLIC_IP: Optional[str] = None
_DEVICE_PUBLIC_IP_TAG: str = "public_ip"


def get_public_ip() -> Optional[str]:
    """Fetch the public IPv4 address using free public services (stdlib only)."""
    urls = [
        "https://api.ipify.org?format=json",
        "https://api.ip.sb/ip",
        "https://httpbin.org/ip",
    ]

    for url in urls:
        try:
            with urllib.request.urlopen(url, timeout=6) as response:
                if "ipify" in url or "httpbin" in url:
                    data = json.load(response)
                    ip = data.get("ip") or data.get("origin")
                    if ip:
                        return ip.split(',')[0].strip()
                else:
                    ip = response.read().decode('utf-8').strip()
                    if ip:
                        return ip
        except Exception:
            continue

    return None


def configure_device_tracking(
    add_public_ip: bool = False,
    tag_key: str = "public_ip",
    ip: Optional[str] = None,
) -> None:
    """Configure global device tracking (called once at config load)."""
    global _DEVICE_PUBLIC_IP, _DEVICE_PUBLIC_IP_TAG
    if add_public_ip and ip:
        _DEVICE_PUBLIC_IP = ip
        _DEVICE_PUBLIC_IP_TAG = tag_key
    else:
        _DEVICE_PUBLIC_IP = None
        _DEVICE_PUBLIC_IP_TAG = "public_ip"


def get_device_public_ip_tag() -> tuple[Optional[str], str]:
    """Return (ip, tag_key) for use in DataPoint."""
    global _DEVICE_PUBLIC_IP, _DEVICE_PUBLIC_IP_TAG
    return _DEVICE_PUBLIC_IP, _DEVICE_PUBLIC_IP_TAG


def escape_lp_identifier(s: str) -> str:
    """Escape measurement, tag key/value, field key for Line Protocol."""
    if not isinstance(s, str):
        s = str(s)
    return (
        s.replace("\\", "\\\\")
         .replace(",", "\\,")
         .replace("=", "\\=")
         .replace(" ", "\\ ")
    )


def escape_lp_field_value(v: Any) -> str:
    """Format and escape a field value for Line Protocol."""
    if isinstance(v, str):
        escaped = v.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    elif isinstance(v, bool):
        return "t" if v else "f"
    elif isinstance(v, int):
        return f"{v}i"
    elif isinstance(v, float):
        return f"{v:g}"
    else:
        raise ValueError(f"Unsupported field value type: {type(v).__name__}")