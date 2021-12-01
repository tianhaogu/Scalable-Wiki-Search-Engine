#!/usr/bin/env python3
"""

Map 2.
doc_id_1  {"term": term_1, "w_ik": num, "tf_ik": int, "idf_k": num}.
doc_id_2  {"term": term_2, "w_ik": num, "tf_ik": int, "idf_k": num}.
doc_id_2  {"term": term_1, "w_ik": num, "tf_ik": int, "idf_k": num}.
Note that doc_id is not unique.
"""
import sys
import json
import math


DOC_COUNT = None

txt_name = "total_document_count.txt"
with open(txt_name, 'r') as countfile:
    count_doc = countfile.readline().strip()
    DOC_COUNT = int(count_doc)


for line in sys.stdin:
    term = line.split("\t")[0]
    term_freq_per_doc_obj = line.split("\t")[1]
    term_freq_per_doc_dict = json.loads(term_freq_per_doc_obj)
    n_k = len(term_freq_per_doc_dict)
    idf_k = math.log(DOC_COUNT / n_k, 10)
    for doc_id, tf_ik in term_freq_per_doc_dict.items():
        w_ik = tf_ik * idf_k
        info_per_doc = {
            "term": term,
            "w_ik": w_ik,
            "tf_ik": tf_ik,
            "idf_k": idf_k
        }
        info_per_doc_obj = json.dumps(info_per_doc)
        print(f"{doc_id}\t{info_per_doc_obj}")
