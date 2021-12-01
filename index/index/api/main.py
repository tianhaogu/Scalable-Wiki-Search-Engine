"""Index Server main code."""
import math
import pathlib
import re
from flask import (jsonify, request)
import index


STOPWORDS_SET = set()
PAGERANK_DICT = {}
INVERTEDINDEX_DICT = {}

@index.app.before_first_request
def startup():
    """Load inverted index, pagerank, and stopwords into memory."""
    index_dir = pathlib.Path(__file__).parent.parent
    read_stopwords(index_dir)
    read_pagerank(index_dir)
    read_inverted_index(index_dir)


def read_stopwords(index_dir):
    """Read the stopwords.txt file."""
    stopwords_file = index_dir / "stopwords.txt"
    with open(str(stopwords_file), 'r') as stopfile:
        for line in stopfile:
            STOPWORDS_SET.add(line.strip())


def read_pagerank(index_dir):
    """Read the pagerank.out file."""
    pagerank_file = index_dir / "pagerank.out"
    with open(str(pagerank_file), 'r') as pagerankfile:
        for line in pagerankfile:
            line = line.strip()
            doc_id, score = line.split(",")
            PAGERANK_DICT[doc_id] = float(score)


def read_inverted_index(index_dir):
    """Read the inverted_index.txt file, based on the configured envvar."""
    index_file_config = index.app.config["INDEX_PATH"]
    inverted_index_file = index_dir / "inverted_index" / index_file_config
    with open(str(inverted_index_file), 'r') as indexfile:
        for line in indexfile:
            term_info_list = line.strip().split()
            term_name = term_info_list[0]
            idf_k = term_info_list[1]
            counter = 2
            term_appears = []
            while counter < len(term_info_list):
                curr_appear = {
                    "doc_id": term_info_list[counter],
                    "tf_ik": int(term_info_list[counter + 1]),
                    "norm": float(term_info_list[counter + 2])
                }
                term_appears.append(curr_appear)
                counter += 3
            term_context = {
                "idf_k": idf_k,
                "term_info_appear": term_appears
            }
            INVERTEDINDEX_DICT[term_name] = term_context


@index.app.route('/api/v1/', methods=["GET"])
def get_index():
    """Return basic information."""
    context = {
        "hits": "/api/v1/hits/",
        "url": "/api/v1/"
    }
    return jsonify(**context)


@index.app.route('/api/v1/hits/', methods=["GET"])
def get_hits():
    """Return hits based on the query."""
    queries = request.query_string.decode('utf-8')
    query = request.args.get("q", default='', type=str)
    weight = request.args.get("w", default=0.5, type=float)
    query_list = process_query(query)
    print(query_list)
    if len(query_list) == 0:
        return index.app.config["EMPTY_HITS"]
    documents_contain = get_documents(query_list)
    if len(documents_contain) == 0:
        return index.app.config["EMPTY_HITS"]
    documents_ranked = rank_documents(query_list, documents_contain, weight)
    documents_ranked_context = []
    for document_info in documents_ranked:
        documents_ranked_context.append({
            "docid": int(document_info[0]),
            "score": document_info[1]
        })
    hit_context = {"hits": documents_ranked_context}
    return jsonify(**hit_context)


def process_query(query):
    """Process and clean the query."""
    query = re.sub(r"[^a-zA-Z0-9 ]+", "", query)
    query = query.strip().casefold()
    query_list = query.split()
    query_list_nostop = []
    for query in query_list:
        if query not in STOPWORDS_SET:
            query_list_nostop.append(query)
    return query_list_nostop


def get_documents(query_list):
    """Get documents that contain all query terms."""
    documents_contain = {}
    for query in query_list:
        if query not in INVERTEDINDEX_DICT:
            return documents_contain
    document_set_list = []
    for query in query_list:
        term_dict = INVERTEDINDEX_DICT[query]
        document_set = set()
        for doc_appear in term_dict["term_info_appear"]:
            document_set.add(doc_appear["doc_id"])
        document_set_list.append(document_set)
    document_set_all = set.intersection(*document_set_list)
    for doc_id in document_set_all:
        document_dict = {}
        for query in query_list:
            term_dict = INVERTEDINDEX_DICT[query]
            term_dict_context = None
            for term_appear_dict in term_dict["term_info_appear"]:
                if term_appear_dict["doc_id"] == doc_id:
                    term_dict_context = {
                        # "idf_k": term_dict["idf_k"],
                        "tf_ik": term_appear_dict["tf_ik"],
                        "norm": term_appear_dict["norm"] 
                    }
            document_dict[query] = term_dict_context
        documents_contain[doc_id] = document_dict
    return documents_contain


def rank_documents(query_list, documents_contain, weight):
    """Rank the gotten documents based on pagerank and tf-idf score."""
    overall_score_list = []
    for doc_id, document_dict in documents_contain.items():
        pagerank_score = calculate_pagerank_score(doc_id)
        tfidf_score = calculate_tfidf_score(query_list, doc_id, document_dict)
        weightd_score = weight * pagerank_score + (1 - weight) * tfidf_score
        overall_score_list.append((doc_id, weightd_score))
    ranked_score_list = sorted(overall_score_list, key=lambda x: (-x[1], x[0]))
    return ranked_score_list


def calculate_pagerank_score(doc_id):
    """Calculate pagerank score."""
    pagerank_score = PAGERANK_DICT[doc_id]
    return pagerank_score


def calculate_tfidf_score(query_list, doc_id, document_dict):
    """Calculate tf-idf score."""
    query_vector = []
    document_vector = []
    for term_name, term_dict in document_dict.items():
        term_freq_in_query = query_list.count(term_name)
        idf_k = float(INVERTEDINDEX_DICT[term_name]["idf_k"])
        query_vector.append(term_freq_in_query * idf_k)
        term_freq_in_doc = term_dict["tf_ik"]
        document_vector.append(term_freq_in_doc * idf_k)
    dot_prod = sum([q * d for q, d in zip(query_vector, document_vector)])
    norm_q_square = 0.0
    for query_norm in query_vector:
        norm_q_square += pow(query_norm, 2)
    norm_q = math.sqrt(norm_q_square)
    norm_d = math.sqrt(float(document_dict[query_list[0]]["norm"]))
    tfidf_score = dot_prod / (norm_q * norm_d)
    return tfidf_score
