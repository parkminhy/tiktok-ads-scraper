thonimport logging
from datetime import datetime
from typing import Any, Optional, Union

logger = logging.getLogger(__name__)

def parse_timestamp_ms(value: Optional[Union[int, float, str]]) -> Optional[int]:
    """
    Parse various timestamp representations into epoch milliseconds.

    Accepts:
    - epoch seconds (int/float)
    - epoch milliseconds (int/float > 10^12)
    - ISO-8601 date strings like "2023-10-15T12:34:56Z"
    - None -> None
    """
    if value is None:
        return None

    # Already an integer timestamp
    if isinstance(value, (int, float)):
        ivalue = int(value)
        # Heuristic: if it's seconds, convert to ms; if it's already ms, keep.
        if ivalue < 10**11:  # before year 5138 in ms
            return ivalue * 1000
        return ivalue

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None

        # Try integer-like string
        if stripped.isdigit():
            try:
                ivalue = int(stripped)
                if ivalue < 10**11:
                    return ivalue * 1000
                return ivalue
            except ValueError:
                pass

        # Try ISO date parsing
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(stripped, fmt)
                # assume UTC if no tzinfo
                if dt.tzinfo is None:
                    ts = int(dt.timestamp() * 1000)
                else:
                    ts = int(dt.timestamp() * 1000)
                return ts
            except ValueError:
                continue

        logger.debug("Unable to parse timestamp from value %r", value)
        return None

    logger.debug("Unsupported timestamp type: %r (%s)", value, type(value))
    return None

def ensure_str(value: Any) -> str:
    """
    Ensure that a value is converted to a clean string.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)

def ensure_int(value: Any, default: int = 0) -> int:
    """
    Ensure that a value is converted to an integer, falling back on default.
    """
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.debug("Unable to convert %r to int; using default=%d", value, default)
        return default