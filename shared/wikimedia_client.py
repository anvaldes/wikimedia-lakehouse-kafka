import requests

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


def open_stream(user_agent, last_event_id=None):
    headers = {"User-Agent": user_agent, "Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    return requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60))


def read_events(response):
    """Yields (event_id, raw_data) for each SSE message. Parsing/validation happens downstream."""
    event_id, data_lines = None, []
    for line in response.iter_lines(decode_unicode=True):
        if line.startswith("id:"):
            event_id = line[3:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].strip())
        elif line == "" and data_lines:
            yield event_id, "\n".join(data_lines)
            event_id, data_lines = None, []


def is_canary(event):
    return event.get("meta", {}).get("domain") == "canary"
