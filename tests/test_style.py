"""Code style tests."""
import subprocess
import utils


def test_pycodestyle():
    """Run pycodestyle."""
    assert_no_prohibited_terms("nopep8", "noqa", "pylint")
    subprocess.run([
        "pycodestyle",
        "index/index",
        "search/search",
    ], check=True)


def test_pydocstyle():
    """Run pydocstyle."""
    assert_no_prohibited_terms("nopep8", "noqa", "pylint")
    subprocess.run([
        "pydocstyle",
        "index", "index/setup.py",
        "search/search",
    ], check=True)


def test_pylint():
    """Run pylint."""
    assert_no_prohibited_terms("nopep8", "noqa", "pylint")
    subprocess.run([
        "pylint",
        "--rcfile", utils.TEST_DIR/"testdata/pylintrc",
        "--disable=cyclic-import",
        "--unsafe-load-any-extension=y",
        "--disable=assigning-non-slot",
        "index/index",
        "search/search",
    ], check=True)


def assert_no_prohibited_terms(*terms):
    """Check for prohibited terms before testing style."""
    for term in terms:
        completed_process = subprocess.run(
            [
                "grep",
                "-r",
                "-n",
                term,
                "--include=*.py",
                "--exclude=__init__.py",
                "--exclude=setup.py",
                "index",
                "search",
            ],
            check=False,  # We'll check the return code manually
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )

        # Grep exit code should be non-zero, indicating that the prohibited
        # term was not found.  If the exit code is zero, crash and print a
        # helpful error message with a filename and line number.
        assert completed_process.returncode != 0, (
            f"The term '{term}' is prohibited.\n{completed_process.stdout}"
        )
