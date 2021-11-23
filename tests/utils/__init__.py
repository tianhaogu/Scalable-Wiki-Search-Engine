"""Autograder utilities."""
import shutil
import pathlib
import pytest
from .cd import CD
from .pipeline import Pipeline
from .hadoop import hadoop


# Directory containing unit tests.  Tests look here for files like inputs.
TEST_DIR = pathlib.Path(__file__).parent.parent

# Default timeout to wait for student code to respond
TIMEOUT = 10

# Tolerance for comparing IDF and normalization values, expressed as a fraction
# of the expected value.  We're using pytest.approx() relative comparison.
# https://docs.pytest.org/en/6.2.x/reference.html#pytest-approx
TOLERANCE = 0.05


def create_and_clean_testdir(tmpdir, basename):
    """Remove tmpdir/basename and then create it."""
    dirname = pathlib.Path(tmpdir)/basename
    if dirname.exists():
        shutil.rmtree(dirname)
    dirname.mkdir(exist_ok=True, parents=True)
    return dirname


def create_and_clean_pipeline_testdir(tmpdir, basename):
    """Copy map and reduce executables and stopwords into clean tmp dir."""
    tmpdir = create_and_clean_testdir(tmpdir, basename)
    inverted_index_dir = pathlib.Path("hadoop/inverted_index")
    for filename in inverted_index_dir.glob("map?.py"):
        shutil.copy(filename, tmpdir)
    for filename in inverted_index_dir.glob("reduce?.py"):
        shutil.copy(filename, tmpdir)
    shutil.copy("hadoop/inverted_index/stopwords.txt", tmpdir)
    return tmpdir


def threesome(iterable):
    """Organize a list in groups of 3.

    Example:
    >>> list(threesome([1, 2, 3, 4, 5, 6]))
    [(1, 2, 3), (4, 5, 6)]

    """
    assert len(list(iterable)) % 3 == 0, "length must be a multiple of 3"
    return zip(*(iter(iterable),) * 3)


def assert_inverted_index_segments_eq(dir1, dir2):
    """Compare two directories of inverted index segments."""
    assert dir1 != dir2, (
        "Refusing to compare a directory to itself:\n"
        f"dir1 = {dir1}\n"
        f"dir2 = {dir2}\n"
    )

    # Get a list of files in each directory, ignoring subdirs
    paths1 = list(dir1.iterdir())
    paths2 = list(dir2.iterdir())
    paths1 = [p for p in paths1 if p.is_file()]
    paths2 = [p for p in paths2 if p.is_file()]

    # Sanity checks
    assert paths1, f"Empty directory: {dir1}"
    assert paths2, f"Empty directory: {dir2}"
    assert len(paths1) == len(paths2), (
        "Number of output files does not match\n"
        f"dir1 = {dir1}\n"
        f"dir2 = {dir2}\n"
        f"number of files in dir1 = {len(paths1)}\n"
        f"number of files in dir2 = {len(paths2)}\n"
    )

    # Compare inverted index segments pairwise
    for path1, path2 in zip(sorted(paths1), sorted(paths2)):
        assert_inverted_index_eq(path1, path2)


def assert_inverted_index_eq(path1, path2):
    """Compare two inverted index files, raising an assertion if different."""
    # We're using lots of variables to construct easy-to-read error messages
    # pylint: disable=too-many-locals

    # Read files into memory
    with path1.open() as infile:
        lines1 = infile.readlines()
    with path2.open() as infile:
        lines2 = infile.readlines()

    # Compare two inverted indexes, line-by-line.  Each line contains one term,
    # one idf value and one or more hits.  Each hit is represented by 3
    # numbers: doc id, number of occurrences of the term, and normalization
    # factor.
    for line1, line2 in zip(lines1, lines2):
        line1 = line1.strip()
        line2 = line2.strip()
        items1 = line1.split()
        items2 = line2.split()

        # Debug message to append to assertions
        debug_info = (
            f"path1 = {str(path1)}\n"
            f"path2 = {str(path2)}\n"
            f"line1 = {line1}\n"
            f"line2 = {line2}\n"
        )

        # Divide one line of inverted index into term, idf, and hits.  Each hit
        # is a represented by three numbers: doc_id, tf, and norm factor.
        term1 = items1[0]
        idf1 = float(items1[1])
        term2 = items2[0]
        idf2 = float(items2[1])
        docs1 = threesome(items1[2:])
        docs2 = threesome(items2[2:])

        # Verify terms match
        debug_info += (
            f"term1 = {term1}\n"
            f"term2 = {term2}\n"
        )
        assert term1 == term2, f"Term mismatch:\n{debug_info}"

        # Verify IDF values match
        debug_info += (
            f"idf1 = {idf1}\n"
            f"idf2 = {idf2}\n"
        )
        assert idf1 == pytest.approx(idf2, TOLERANCE), \
            f"IDF mismatch (tolerance={TOLERANCE}):\n{debug_info}"

        # Verify each hit matches.  One hit contains doc_id, tf and norm factor
        for hit1, hit2 in zip(docs1, docs2):
            doc_id1 = int(hit1[0])
            tf1 = float(hit1[1])
            norm1 = float(hit1[2])
            doc_id2 = int(hit2[0])
            tf2 = float(hit2[1])
            norm2 = float(hit2[2])

            # Compare doc ids
            debug_info += (
                "\n"
                f"doc_id1 = {doc_id1}\n"
                f"doc_id2 = {doc_id2}\n"
            )
            assert doc_id1 == doc_id2, f"doc_id mismatch:\n{debug_info}"

            # Compare tf
            debug_info += (
                f"tf1 = {tf1}\n"
                f"tf2 = {tf2}\n"
            )
            assert tf1 == tf2, f"tf mismatch:\n{debug_info}"

            # compare normalization factors
            debug_info += (
                f"norm1 = {norm1}\n"
                f"norm2 = {norm2}\n"
            )
            assert norm1 == pytest.approx(norm2, TOLERANCE), (
                f"Normalization factor mismatch (tolerance={TOLERANCE})\n"
                f"{debug_info}"
            )

    # Verify correct number of terms
    assert len(lines1) == len(lines2), (
        f"Number of lines mismatch:\n"
        f"path1 = {str(path1)}\n"
        f"path2 = {str(path2)}\n"
        f"len(lines1) = {len(lines1)}\n"
        f"len(lines2) = {len(lines2)}\n"
    )


def assert_compare_hits(hits1, hits2):
    """Compare two lists of hits, raising an assertion on mismatch."""
    for hit1, hit2 in zip(hits1, hits2):
        assert "docid" in hit1
        assert "docid" in hit2
        assert "score" in hit1
        assert "score" in hit2
        assert hit1["docid"] == hit2["docid"], (
            "docid mismatch:\n"
            f"hit1 = {hit1}\n"
            f"hit2 = {hit2}\n"
        )
        assert hit1["score"] == pytest.approx(hit2["score"], TOLERANCE), (
            "score mismatch (TOLERANCE={TOLERANCE}):\n"
            f"hit1 = {hit1}\n"
            f"hit2 = {hit2}\n"
        )
    assert len(hits1) == len(hits2), (
        "Length mismatch:\n"
        f"len(hits1) = {len(hits1)}\n"
        f"len(hits2) = {len(hits2)}\n"
    )
