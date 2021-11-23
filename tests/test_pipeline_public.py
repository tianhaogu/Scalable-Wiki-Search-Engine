"""Public Inverted Index MapReduce pipeline tests."""

from pathlib import Path
import shutil
import utils
from utils import TEST_DIR


def test_doc_count_one_mapper():
    """Doc count MapReduce job with one input, resulting in one map task."""
    utils.create_and_clean_testdir("tmp", "test_doc_count_one_mapper")

    # Copy executables
    shutil.copy(
        "hadoop/inverted_index/map0.py",
        "tmp/test_doc_count_one_mapper/",
    )
    shutil.copy(
        "hadoop/inverted_index/reduce0.py",
        "tmp/test_doc_count_one_mapper/",
    )

    # Run MapReduce job
    with utils.CD("tmp/test_doc_count_one_mapper"):
        utils.hadoop(
            input_dir=TEST_DIR/"testdata/test_doc_count_one_mapper/input",
            output_dir="output",
            map_exe="./map0.py",
            reduce_exe="./reduce0.py",
        )

    # Verify doc count
    doc_count_path = "tmp/test_doc_count_one_mapper/output/part-00000"
    doc_count_str = Path(doc_count_path).read_text(encoding='utf-8')
    doc_count = int(doc_count_str)
    assert doc_count == 3


def test_num_phases():
    """There should be more than one MapReduce program in the pipeline."""
    with utils.CD("hadoop/inverted_index/"):
        mapper_exes, reducer_exes = utils.Pipeline.get_exes()
    num_mappers = len(mapper_exes)
    num_reducers = len(reducer_exes)
    assert num_mappers > 1, "Must use more than 1 map phase"
    assert num_reducers > 1, "Must use more than 1 reduce phase"


def test_simple():
    """Simple input with no stopwords, no uppercase and no alphanumerics.

    A basic document with no stopwords, upppercase letters, numbers,
    non-alphanumeric characters, or repeated words.

    """
    tmpdir = utils.create_and_clean_pipeline_testdir(
        "tmp",
        "test_pipeline03",
    )

    # Set total document count to be 2
    doc_count_filename = tmpdir/"total_document_count.txt"
    Path(doc_count_filename).write_text("2", encoding='utf-8')

    # Start pipeline mapreduce job, with 1 mapper and 1 reducer
    with utils.CD(tmpdir):
        pipeline = utils.Pipeline(
            input_dir=TEST_DIR/"testdata/test_pipeline03/input",
            output_dir="output",
        )

        # Concatenate output files to output.txt
        output_filename = pipeline.get_output_concat()

    # Verify output
    utils.assert_inverted_index_eq(
        output_filename,
        TEST_DIR/"testdata/test_pipeline03/expected.txt",
    )


def test_uppercase():
    """Input with uppercase characters.

    This test checks if students handle upper case characters correctly,
    they should essentially be replaced with lower case characters.  There
    are no stopwords, numbers or non-alphanumeric characters present in
    this test.

    """
    tmpdir = utils.create_and_clean_pipeline_testdir(
        "tmp",
        "test_pipeline04",
    )

    # Set total document count to be 2
    doc_count_filename = tmpdir/"total_document_count.txt"
    Path(doc_count_filename).write_text("2", encoding='utf-8')

    # Run pipeline mapreduce job with 1 mapper and 1 reducer
    with utils.CD(tmpdir):
        pipeline = utils.Pipeline(
            input_dir=TEST_DIR/"testdata/test_pipeline04/input",
            output_dir="output",
        )
        output_filename = pipeline.get_output_concat()

    # Verify output
    utils.assert_inverted_index_eq(
        output_filename,
        TEST_DIR/"testdata/test_pipeline04/expected.txt",
    )

    # Run same job with 3 mappers and 3 reducers
    with utils.CD(tmpdir):
        pipeline = utils.Pipeline(
            input_dir=TEST_DIR/"testdata/test_pipeline04/input_multi",
            output_dir="output_multi",
        )
        output_filename = pipeline.get_output_concat()

    # Verify output
    utils.assert_inverted_index_eq(
        output_filename,
        TEST_DIR/"testdata/test_pipeline04/expected.txt",
    )


def test_uppercase_and_numbers():
    """Input with uppercase characters and numbers.

    This test checks that students are handling numbers correctly, which means
    leaving them inside the word they are a part of. This test also contains
    upper case characters. There are no stopwords or non-alphanumeric
    characters present in this test.

    """
    tmpdir = utils.create_and_clean_pipeline_testdir(
        "tmp",
        "test_pipeline05",
    )

    # Set total document count to be 2
    doc_count_filename = tmpdir/"total_document_count.txt"
    Path(doc_count_filename).write_text("2", encoding='utf-8')

    # Run pipeline mapreduce job with 1 mapper and 1 reducer
    with utils.CD(tmpdir):
        pipeline = utils.Pipeline(
            input_dir=TEST_DIR/"testdata/test_pipeline05/input",
            output_dir="output",
        )
        output_filename = pipeline.get_output_concat()

    # Verify output
    utils.assert_inverted_index_eq(
        output_filename,
        TEST_DIR/"testdata/test_pipeline05/expected.txt",
    )

    # Rerun with multiple mappers and reducers
    with utils.CD(tmpdir):
        pipeline = utils.Pipeline(
            input_dir=TEST_DIR/"testdata/test_pipeline05/input_multi",
            output_dir="tmp/test_pipeline05/output_multi",
        )
        output_filename = pipeline.get_output_concat()

    # Verify output
    utils.assert_inverted_index_eq(
        output_filename,
        TEST_DIR/"testdata/test_pipeline05/expected.txt",
    )


def test_non_alphanumeric():
    """Input with non-alphanumeric characters.

    This test checks that students are handling non-alphanumeric characters
    properly, i.e., removing them from the word. If a token contains only
    non-alphanumeric characters then it is omitted from the inverted
    index. There are uppercase characters and numbers in this test. There are
    no stopwords.

    """
    tmpdir = utils.create_and_clean_pipeline_testdir(
        "tmp",
        "test_pipeline06",
    )

    # Set total document count to be 2
    doc_count_filename = tmpdir/"total_document_count.txt"
    Path(doc_count_filename).write_text("2", encoding='utf-8')

    # Run pipeline mapreduce job with 1 mapper and 1 reducer
    with utils.CD(tmpdir):
        pipeline = utils.Pipeline(
            input_dir=TEST_DIR/"testdata/test_pipeline06/input",
            output_dir="output",
        )
        output_filename = pipeline.get_output_concat()

    # Verify output
    utils.assert_inverted_index_eq(
        output_filename,
        TEST_DIR/"testdata/test_pipeline06/expected.txt",
    )

    # Rerun with multiple mappers and reducers
    with utils.CD(tmpdir):
        pipeline = utils.Pipeline(
            input_dir=TEST_DIR/"testdata/test_pipeline06/input_multi",
            output_dir="output_multi",
        )
        output_filename = pipeline.get_output_concat()

    # Verify output
    utils.assert_inverted_index_eq(
        output_filename,
        TEST_DIR/"testdata/test_pipeline06/expected.txt",
    )


def test_many_docs():
    """Term appears in  multiple documents.

    This test checks that students are properly handling the case of a term
    appearing in multiple documents. The inverted index entry should contain a
    chain of document ids.

    """
    tmpdir = utils.create_and_clean_pipeline_testdir(
        "tmp",
        "test_pipeline10",
    )

    # Set total document count to be 3
    doc_count_filename = tmpdir/"total_document_count.txt"
    Path(doc_count_filename).write_text("3", encoding='utf-8')

    # Run pipeline mapreduce job
    with utils.CD(tmpdir):
        pipeline = utils.Pipeline(
            input_dir=TEST_DIR/"testdata/test_pipeline10/input_multi",
            output_dir="output_multi",
        )
        output_dir = pipeline.get_output_dir()

    # There should be two output inverted index as docids 25 and 31 will go in
    # one inverted index and docid 30 will go in another.
    utils.assert_inverted_index_segments_eq(
        output_dir,
        TEST_DIR/"testdata/test_pipeline10/expected",
    )


def test_segments():
    """Output is segmented by document.

    This test checks that students are properly distributing documents across
    the output inverted indexes.

    """
    tmpdir = utils.create_and_clean_pipeline_testdir(
        "tmp",
        "test_pipeline14",
    )

    # Set total document count to be 10
    doc_count_filename = tmpdir/"total_document_count.txt"
    Path(doc_count_filename).write_text("10", encoding='utf-8')

    # Run pipeline mapreduce job
    with utils.CD(tmpdir):
        pipeline = utils.Pipeline(
            input_dir=TEST_DIR/"testdata/test_pipeline14/input_multi",
            output_dir="output",
        )
        output_dir = pipeline.get_output_dir()

    # Verify output
    utils.assert_inverted_index_segments_eq(
        output_dir,
        TEST_DIR/"testdata/test_pipeline14/expected",
    )
