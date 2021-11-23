"""Utility and init script tests."""
import shutil
import time
import subprocess
from pathlib import Path
import pytest
import utils


# Time to wait for server to start
TIMEOUT = 10

# This pylint warning is endemic to pytest.
# pylint: disable=unused-argument


@pytest.fixture(name="setup_teardown")
def setup_teardown_fixture():
    """Set up the test and cleanup after."""
    # Setup code: make sure no stale processes are running
    assert not pgrep("flask"), \
        "Found running flask process.  Try 'pkill -f flask'"

    # Transfer control to testcase
    yield None

    # Teardown: kill any stale processes
    pkill("flask")
    assert wait_for_flask_stop()


def test_executables(setup_teardown):
    """Verify bin/index, bin/search, bin/indexdb are shell scripts."""
    assert_is_shell_script("bin/install")
    assert_is_shell_script("bin/search")
    assert_is_shell_script("bin/index")
    assert_is_shell_script("bin/indexdb")


def test_install():
    """Verify install script contains the right commands."""
    install_content = Path("bin/install").read_text(encoding='utf-8')
    assert "python3 -m venv" in install_content
    assert "source env/bin/activate" in install_content
    assert "pip install -r search/requirements.txt" in install_content
    assert "pip install -e search" in install_content
    assert "pip install -r index/requirements.txt" in install_content
    assert "pip install -e index" in install_content
    assert "ln -sf ../../tests/utils/hadoop.py hadoop" in install_content


def test_servers_start(setup_teardown):
    """Verify index and search servers start."""
    # We need to use subprocess.run() on commands that will return non-zero
    # pylint: disable=subprocess-run-check

    # Try to start search server with missing index server
    completed_process = subprocess.run(["bin/search", "start"])
    assert completed_process.returncode != 0

    # Try to start index server with missing database
    db_path = Path("search/search/var/index.sqlite3")
    if db_path.exists():
        db_path.unlink()
    completed_process = subprocess.run(["bin/index", "start"])
    assert completed_process.returncode != 0

    # Create database
    db_path.parent.mkdir(exist_ok=True)
    shutil.copy(utils.TEST_DIR/"testdata/index.sqlite3", db_path)

    # Start index server, which should start 3 Flask processes
    subprocess.run(["bin/index", "start"], check=True)
    assert wait_for_flask_start(nprocs=3)

    # Try to start index server when it's already running
    completed_process = subprocess.run(["bin/index", "start"])
    assert completed_process.returncode != 0

    # Start search server
    subprocess.run(["bin/search", "start"], check=True)
    assert wait_for_flask_start(nprocs=4)

    # Try to start search server when it's already running
    completed_process = subprocess.run(["bin/search", "start"])
    assert completed_process.returncode != 0


def test_servers_stop(setup_teardown):
    """Verify index and search servers start."""
    # Start servers
    subprocess.run(["bin/index", "start"], check=True)
    subprocess.run(["bin/search", "start"], check=True)
    assert wait_for_flask_start(nprocs=4)

    # Stop servers
    subprocess.run(["bin/index", "stop"], check=True)
    subprocess.run(["bin/search", "stop"], check=True)
    assert wait_for_flask_stop()


def test_servers_status(setup_teardown):
    """Verify index and search init script status subcommand."""
    # We need to use subprocess.run() on commands that will return non-zero
    # pylint: disable=subprocess-run-check

    # Create database
    db_path = Path("search/search/var/index.sqlite3")
    db_path.parent.mkdir(exist_ok=True)
    shutil.copy(utils.TEST_DIR/"testdata/index.sqlite3", db_path)

    # Verify status stopped
    completed_process = subprocess.run(["bin/index", "status"])
    assert completed_process.returncode != 0
    completed_process = subprocess.run(["bin/search", "status"])
    assert completed_process.returncode != 0

    # Start index and check status
    subprocess.run(["bin/index", "start"], check=True)
    assert wait_for_flask_start(nprocs=3)
    completed_process = subprocess.run(["bin/index", "status"])
    assert completed_process.returncode == 0

    # Start search and check status
    subprocess.run(["bin/search", "start"], check=True)
    assert wait_for_flask_start(nprocs=4)
    completed_process = subprocess.run(["bin/search", "status"])
    assert completed_process.returncode == 0

    # Stop servers
    subprocess.run(["bin/index", "stop"], check=True)
    subprocess.run(["bin/search", "stop"], check=True)
    assert wait_for_flask_stop()


def test_indexdb_script(setup_teardown):
    """Test the indexdb script."""
    # Create tmp directory containing search/search/sql/index.sql
    utils.create_and_clean_testdir("tmp", "test_index_db_script")
    Path("tmp/test_index_db_script/search/search/sql").mkdir(parents=True)
    shutil.copy(
        utils.TEST_DIR/"testdata/small.sql",
        "tmp/test_index_db_script/search/search/sql/index.sql",
    )

    # Run indexdb script inside tmp directory
    indexdb_path = Path("bin/indexdb").resolve()
    subprocess.run(
        [indexdb_path, "reset"],
        cwd="tmp/test_index_db_script",
        check=True,
    )

    # Verify search/search/var/index.sqlite3 was created
    assert Path(
        "tmp/test_index_db_script/search/search/var/index.sqlite3"
    ).exists()


def pgrep(pattern):
    """Return list of matching processes."""
    completed_process = subprocess.run(
        ["pgrep", "-f", pattern],
        check=False,  # We'll check the return code manually
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )
    if completed_process.returncode == 0:
        return completed_process.stdout.strip().split("\n")
    return []


def pkill(pattern):
    """Issue a "pkill -f pattern" command, ignoring the exit code."""
    subprocess.run(["pkill", "-f", pattern], check=False)


def assert_is_shell_script(path):
    """Assert path is an executable shell script."""
    path = Path(path)
    assert path.exists()
    output = subprocess.run(
        ["file", path],
        check=True, stdout=subprocess.PIPE, universal_newlines=True,
    ).stdout
    assert "shell script" in output
    assert "executable" in output


def wait_for_flask_start(nprocs):
    """Wait for nprocs Flask processes to start running."""
    # Need to check for processes twice to make sure that
    # the flask processes doesn't error out but get marked correct
    count = 0
    for _ in range(TIMEOUT):
        if len(pgrep("flask")) == nprocs:
            count += 1
        if count >= 2:
            return True
        time.sleep(1)
    return False


def wait_for_flask_stop():
    """Wait for Flask servers to stop running."""
    for _ in range(TIMEOUT):
        if not pgrep("flask"):
            return True
        time.sleep(1)
    return False
