#!/usr/bin/env python3
"""

Map 3.
reduced_id1  {"term_list": [{"term": term1, "idf_k": num, "tf_ik": int, "norm": num, "doc_id": int}, {"term": term3, }, ...]}.
reduced_id2  {"term_list": [{"term": term3, "idf_k": num, "tf_ik": int, "norm": num, "doc_id": int}, {"term": term6, }, ...]}.
Totally 3 reduced_id's.
"""
import sys
import json


NUM_REDUCE_TASKS = 3

for line in sys.stdin:
    doc_id = line.split("\t")[0]
    info_per_doc = line.split("\t")[1]
    info_per_doc_dict = json.loads(info_per_doc)
    reduced_id = int(doc_id) % NUM_REDUCE_TASKS
    for i in range(len(info_per_doc_dict["term_list"])):
        info_per_doc_dict["term_list"][i]["norm"] = info_per_doc_dict["norm"]
        info_per_doc_dict["term_list"][i]["doc_id"] = doc_id
    info_per_doc_dict.pop("norm", None)
    info_per_doc_obj = json.dumps(info_per_doc_dict)
    print(f"{reduced_id}\t{info_per_doc_obj}")
