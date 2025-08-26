from datetime import datetime
from typing import Any, Dict

def enrich_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Enrich a record with computed fields.

    Args:
        record (Dict[str, Any]): Original record to process.

    Returns:
        Dict[str, Any]: Transformed record containing additional fields.
    """
    result = dict(record)
    result["processed_at"] = datetime.utcnow().isoformat()
    value = record.get("value")
    if isinstance(value, (int, float)):
        result["value_squared"] = value ** 2
    return result
