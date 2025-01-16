from dataclasses import dataclass, field
from typing import Any, Dict, Optional

class ErrorResult:
    def __init__(self, url, error_message, **kwargs):
        self.url = url
        self.error_message = error_message
        self.data = {"error": error_message}


@dataclass
class Result:
    status_code: int
    message: Optional[str]
    data: Optional[Dict | Any] = field(default_factory=dict)
    headers: Optional[Dict | Any] = field(default_factory=dict)
    pagination: Optional[Dict | Any] = field(default_factory=dict)

    def __post_init__(self):
        self.pagination = self._extract_pagination()

    def _extract_pagination(self) -> Optional[Dict[str, Any]]:
        if not self.data:
            return None
        pagination = self.data.get('pagination', {})
        if pagination:
            return {
            'count': pagination.get('count'),
            'next': pagination.get('next') or pagination.get('nextPage'),
            'previous': pagination.get('previous') or pagination.get('previousPage')
        }
        else:
            # remove message, offset, count, pageSize, offsetMark, nextPage, previousPage from data
            pagination = {
                "count": self.data.get("count"),
                "next": self.data.get("nextPage"),
                "previous": self.data.get("previousPage")
            }
            for key in ["message", "offset", "count", "pageSize", "offsetMark", "nextPage", "previousPage"]:
                self.data.pop(key, None)
            # print(pagination)
            return pagination



