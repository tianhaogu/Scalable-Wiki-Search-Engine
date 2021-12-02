#!/usr/bin/env python3
"""

Reduce 2.
doc_id1  {"norm": num, "term_list": [{"term": term1, "idf_k": num, "tf_ik": int}, {"term": term2, ...}, ...]}.
doc_id2  {"norm": num, "term_list": [{"term": term2, "idf_k": num, "tf_ik": int}, {"term": term4, ...}, ...]}.
Note that doc_id is unique.
"""
import sys
import json
import itertools


TOTAL_NORM_PER_DOC = {}
TERMS_PER_DOC = {}

def reduce_one_group(key, group):
    terms_list = []
    for line in group:
        info_per_doc_obj = line.partition("\t")[2]
        info_per_doc_dict = json.loads(info_per_doc_obj)
        w_ik = info_per_doc_dict["w_ik"]
        if key in TOTAL_NORM_PER_DOC:
            TOTAL_NORM_PER_DOC[key] += pow(w_ik, 2)
        else:
            TOTAL_NORM_PER_DOC[key] = pow(w_ik, 2)
        terms_list.append(info_per_doc_dict)
    TERMS_PER_DOC[key] = terms_list


def integrate_group(key, group):
    term_list = []
    for line in group:
        info_per_doc_dict = line
        info_per_doc_dict.pop("w_ik", None)
        term_list.append(info_per_doc_dict)
    output_per_doc = {
        "norm": TOTAL_NORM_PER_DOC[key],
        "term_list": term_list
    }
    output_per_doc_obj = json.dumps(output_per_doc)
    print(f"{key}\t{output_per_doc_obj}")


def keyfunc(line):
    return line.partition("\t")[0]


def main():
    for key, group in itertools.groupby(sys.stdin, keyfunc):
        reduce_one_group(key, group)
    for key, group in TERMS_PER_DOC.items():
        integrate_group(key, group)


if __name__ == "__main__":
    main()
