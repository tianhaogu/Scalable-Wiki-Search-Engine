"""Shared test fixtures.

Pytest fixture docs:
https://docs.pytest.org/en/latest/fixture.html#conftest-py-sharing-fixture-functions
"""

import time
import shutil
from pathlib import Path
import logging
import socket
import urllib
import multiprocessing
import importlib
import os
import types
import sys
import pytest
import utils
import index
import search


# Set up logging
LOGGER = logging.getLogger("autograder")

# How long to wait for server in separate thread to start or stop
SERVER_START_STOP_TIMEOUT = 5

# Inverted index segment filenames from index/index/inverted_index/
INDEX_PATHS = [
    "inverted_index_0.txt",
    "inverted_index_1.txt",
    "inverted_index_2.txt",
]


@pytest.fixture(name='search_client')
def setup_teardown_search_client():
    """Start a Search Server and Index Servers.

    The Search Server is a Flask test server.  The three Index Servers are live
    servers run in separate processes.

    """
    LOGGER.info("Setup test fixture 'search_client'")

    # Reset database
    db_path = Path("search/search/var/index.sqlite3")
    db_path.parent.mkdir(exist_ok=True)
    shutil.copy(utils.TEST_DIR/"testdata/index.sqlite3", db_path)

    # Configure Flask app.  Testing mode so that exceptions are propagated
    # rather than handled by the the app's error handlers.
    search.app.config["TESTING"] = True

    # Start Index Servers in different processes.  We need different processes
    # because each Index Server loads a segment into memory as a module level
    # variable.  Therefore, we can't have one version of the index module in
    # memory.  Port select is automatic.
    live_index_servers = []
    for index_path in INDEX_PATHS:
        assert (Path("index/index/inverted_index")/index_path).exists()
        live_index_server = LiveIndexServer(index_path)
        live_index_server.start()
        live_index_servers.append(live_index_server)

    # Wait for Index Servers to start
    for live_index_server in live_index_servers:
        live_index_server.wait_for_urlopen()

    # Start Search Server, configured to connect to Index Server API URLs.
    assert "SEARCH_INDEX_SEGMENT_API_URLS" in search.app.config, \
        "Can't find SEARCH_INDEX_SEGMENT_API_URLS in Search Server config."
    api_urls = [i.hits_api_url() for i in live_index_servers]
    search.app.config["SEARCH_INDEX_SEGMENT_API_URLS"] = api_urls

    # Transfer control to test.  The code before the "yield" statement is setup
    # code, which is executed before the test.  Code after the "yield" is
    # teardown code, which is executed at the end of the test.  Teardown code
    # is executed whether the test passed or failed.
    with search.app.test_client() as client:
        yield client

    # Stop Index Servers
    LOGGER.info("Teardown test fixture 'search_client'")
    for live_index_server in live_index_servers:
        live_index_server.stop()


@pytest.fixture(name="index_client")
def setup_teardown_index_client():
    """
    Start a Flask test server for one Index Server with segment 0.

    This fixture is used to test the REST API, it won't start a live server.

    Flask docs: https://flask.palletsprojects.com/en/1.1.x/testing/#testing
    """
    LOGGER.info("Setup test fixture 'index_client'")

    # Configure Flask app.  Testing mode so that exceptions are propagated
    # rather than handled by the the app's error handlers.
    index.app.config["TESTING"] = True

    # Set environment variable and reload module. The index server loads the
    # inverted index into memory as a module-level variable.  When the module
    # reloads, it may reload the inverted index.
    segment_path = INDEX_PATHS[0]
    assert (Path("index/index/inverted_index")/segment_path).exists()
    os.environ["INDEX_PATH"] = segment_path
    rreload(index, path="index/index/api")

    # Transfer control to test.  The code before the "yield" statement is setup
    # code, which is executed before the test.  Code after the "yield" is
    # teardown code, which is executed at the end of the test.  Teardown code
    # is executed whether the test passed or failed.
    with index.app.test_client() as client:
        yield client

    # Teardown code starts here
    LOGGER.info("Teardown test fixture 'index_client'")


class LiveIndexServerError(Exception):
    """Exception type used by LiveIndexServer."""


class LiveIndexServer:
    """Run an Index Server in a separate process.

    We need a separate process because the Index Server loads the inverted
    index into memory as a module level variable.  We want to run multiple
    Index Servers, each with a different segment loaded into memory.  If we
    used threads, then the different instances would share one data structure.

    """

    def __init__(self, index_path, port=None):
        """Store parameters."""
        self.index_path = index_path
        self.port = self.get_open_port() if port is None else port
        self.process = None

    def __str__(self):
        """Return string describing this instance."""
        return f"LiveIndexServer({self.url()}, {self.index_path})"

    def run_flask_app(self):
        """Run flask app with modified environment variables."""
        os.environ["INDEX_PATH"] = self.index_path
        rreload(index, path="index/index/api")
        index.app.run(
            port=self.port,
            debug=False,
            use_reloader=False,
            threaded=False,
        )

    def url(self):
        """Return base URL of running server."""
        return f"http://localhost:{self.port}/"

    def hits_api_url(self):
        """Return REST API URL for the hits route."""
        url = urllib.parse.urljoin(self.url(), "/api/v1/hits/")
        return url

    def start(self):
        """Start Flask server in separate process."""
        self.process = multiprocessing.Process(
            target=self.run_flask_app,
            name=str(self),
            daemon=True,
        )
        self.process.start()

    def stop(self):
        """Stop Flask server process."""
        self.process.terminate()

    @staticmethod
    def get_open_port():
        """Return a port that is available for use on localhost."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(('', 0))
            port = sock.getsockname()[1]
        return port

    def wait_for_urlopen(self):
        """Wait for server to respond, return False if it times out."""
        for _ in range(10*SERVER_START_STOP_TIMEOUT):
            try:
                with urllib.request.urlopen(self.url()):
                    return
            except urllib.error.HTTPError as err:
                # Server returned a response.  Anything that's not an HTTP
                # error (5xx) indicate a working server.
                if err.code < 500:
                    return
                raise LiveIndexServerError(
                    f"{self} GET {self.url()} {err.code}"
                ) from err
            except urllib.error.URLError:
                pass
            if self.process.exitcode is not None:
                raise LiveIndexServerError(f"Premature exit: {self}")
            time.sleep(0.1)
        raise LiveIndexServerError(f"Failed to start: {self}")


def rreload(module, path=None):
    """Recursively reload module.

    We use this function to reload the index segment server, which maintains a
    copy of an inverted index in memory as a module-level variable.  When a
    test changes the inverted index, we need to reload the index segment
    module.

    importlib's reload function does not recursively reload packages.  If we
    only reload the index.index module we get a new flask app that has no
    attached routes as the index.index.api package does not get reloaded. We
    fixed this by writing our own recursive reload function.

    Ref: https://stackoverflow.com/questions/15506971/
    """
    importlib.reload(module)
    for attribute_name in dir(module):
        attribute = getattr(module, attribute_name)
        if isinstance(attribute, types.ModuleType):
            if attribute.__name__ not in sys.builtin_module_names:
                if path in os.path.dirname(attribute.__file__):
                    rreload(attribute, path)
