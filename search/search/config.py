"""Search Server development configuration."""
import pathlib


SEARCH_SERVER_ROOT = pathlib.Path(__file__).resolve().parent.parent

DATABASE_FILENAME = SEARCH_SERVER_ROOT / "search" / "var" / "index.sqlite3"

SEARCH_INDEX_SEGMENT_API_URLS = [
    "http://localhost:9000/api/v1/hits/",
    "http://localhost:9001/api/v1/hits/",
    "http://localhost:9002/api/v1/hits/",
]

HIT_CONTEXT_LIST = []
