"""Search Server main code."""
import threading
from flask import (jsonify, request)
import search


@search.app.route('/', methods=['GET'])
def get_search():
    """Display search results based on the query."""
    query = request.form['q']
    weight = request.form['w']
    connection = search.model.get_db()
    hit_urls = generate_urls(query, weight)
    threads = []
    for hit_url in hit_urls:
        thread = threading.Thread(target=send_request_get, args=(hit_url))
        threads.append(thread)
        thread.start()


def generate_urls(query, weight):
    """Generate the url used to be passed to the index server."""
    queries = query.split()
    final_query = '+'.join(queries)
    url_postfix = f"?q={final_query}&w={weight}"
    search_api_urls = search.app.config["SEARCH_INDEX_SEGMENT_API_URLS"]
    url_list = [(url_prefix + url_postfix) for url_prefix in search_api_urls]
    return url_list


def send_request_get(hit_url):
    """Send the hit url to the index server."""
    hit_context = requests.get(hit_url)
    hit_context_json = hit_context.json()
    search.app.config["HIT_CONTEXT_LIST"].append(hit_context_json)
