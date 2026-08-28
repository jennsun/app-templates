from datetime import datetime
from typing import Any, Dict, Optional


_UNSET = object()


def _without_none(values: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in values.items() if value is not None}


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None or isinstance(value, datetime):
        return value
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)
