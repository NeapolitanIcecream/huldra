from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from huldra.models import ArxivRequest


def build_submitted_date_windows(
    *,
    search_queries: list[str],
    start_date: date,
    end_date: date,
    max_results: int,
    client_id: str = "huldra-backfill",
) -> list[ArxivRequest]:
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    normalized_queries = [query.strip() for query in search_queries if query.strip()]
    if not normalized_queries:
        raise ValueError("search_queries cannot be empty")
    requests: list[ArxivRequest] = []
    current = start_date
    while current <= end_date:
        start = datetime(current.year, current.month, current.day, tzinfo=UTC)
        end = start + timedelta(days=1)
        for query in normalized_queries:
            requests.append(
                ArxivRequest(
                    client_id=client_id,
                    search_query=query,
                    submitted_start=start,
                    submitted_end=end,
                    max_results=max_results,
                )
            )
        current += timedelta(days=1)
    return requests
