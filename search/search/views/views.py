"""Search Server main code."""
import threading
import requests
import heapq
from flask import (jsonify, request, render_template)
import search


@search.app.route('/', methods=['GET'])
def get_search():
    """Display search results based on the query."""
    query = request.args.get('q', default='', type=str)
    weight = request.args.get('w', default=0.5, type=float)
    connection = search.model.get_db()
    hit_urls = generate_urls(query, weight)
    threads = []
    for hit_url in hit_urls:
        thread = threading.Thread(target=send_request_get, args=(hit_url,))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    config_hit = search.app.config["HIT_CONTEXT_LIST"]
    print(config_hit)
    top_ten_docs_info = []
    for rank, doc_dict in enumerate(
            heapq.merge(*config_hit, key=lambda x: x["score"], reverse=True)):
        if rank >= 10:
            break
        curr_doc = get_doc_info(doc_dict, connection)
        top_ten_docs_info.append(curr_doc)
    search_context = {
        "top_ten_docs": top_ten_docs_info,
        "query": query,
        "weight": weight if weight not in (0.0, 1.0) \
                else '0' if weight == 0.0 else '1'
    }
    search.app.config["HIT_CONTEXT_LIST"].clear()
    return render_template("index.html", **search_context)


def generate_urls(query, weight):
    """Generate the url used to be passed to the index server."""
    queries = query.split()
    final_query = '+'.join(queries)
    url_postfix = f"?q={final_query}&w={weight}" if weight \
            else f"?q={final_query}"
    search_api_urls = search.app.config["SEARCH_INDEX_SEGMENT_API_URLS"]
    url_list = [(url_prefix + url_postfix) for url_prefix in search_api_urls]
    return url_list


def send_request_get(hit_url):
    """Send the hit url to the index server, and get top 10 of each server."""
    hit_context = requests.get(hit_url)
    hit_context_json = hit_context.json()
    top_ten_hit_list = hit_context_json["hits"][0:10]
    search.app.config["HIT_CONTEXT_LIST"].append(top_ten_hit_list)


def get_doc_info(hit_result, connection):
    """Get doc_url, doc_title and doc_summary given the hit result (doc_id)."""
    doc_result = connection.execute(
        "SELECT url, title, summary FROM Documents WHERE docid = ?",
        (hit_result["docid"],)
    )
    curr_doc = doc_result.fetchone()
    return curr_doc
