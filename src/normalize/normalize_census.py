from typing import Dict, Any, List

def normalize_abscs(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Census JSON (2D array) -> rows
    """
    data = payload.get("data", [])
    if not data or len(data) < 2:
        return []

    header = data[0]
    rows = []
    for r in data[1:]:
        row = dict(zip(header, r))
        rows.append(row)
    return rows
