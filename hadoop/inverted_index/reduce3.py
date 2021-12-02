#!/usr/bin/env python3
"""

Reduce 3.
reduced_id1  {term1: {"idf_k": num, "term_info_appear": [{"doc_id": str, "tf_ik": int, "norm": num}, {}]}, term2: {  }, ...}.
reduced_id2  {term1: {"idf_k": num, "term_info_appear": [{"doc_id": str, "tf_ik": int, "norm": num}, {}]}, term4: {  }, ...}.
"""
import sys
import json
import itertools


def output_one_group(current_file_terms_sorted, output_file):
    for (term, term_info) in current_file_terms_sorted:
        idf_k = term_info["idf_k"]
        appear_str_list = []
        term_info_appear_list = term_info["term_info_appear"]
        term_info_appear_list_sorted = sorted(
            term_info_appear_list, key=lambda dict: dict["doc_id"]
        )
        for new_term_dict in term_info_appear_list_sorted:
            current_str = ' '.join(new_term_dict.values())
            appear_str_list.append(current_str)
        all_appear_str = ' '.join(appear_str_list)
        print(f"{term} {idf_k} {all_appear_str}")


def reduce_one_group(key, group):
    output_file = f"part-0000{key}"
    current_file_terms = {}
    for line in group:
        info_per_doc = line.partition("\t")[2]
        info_per_doc_dict = json.loads(info_per_doc)
        for term_dict in info_per_doc_dict["term_list"]:
            term_name = term_dict["term"]
            new_term_dict = {
                "doc_id": str(term_dict["doc_id"]),
                "tf_ik": str(term_dict["tf_ik"]),
                "norm": str(term_dict["norm"])
            }
            if term_name not in current_file_terms:
                current_file_terms[term_name] = {
                    "idf_k": str(term_dict["idf_k"]),
                    "term_info_appear": [new_term_dict]
                }
            else:
                current_file_terms[term_name]["term_info_appear"].append(
                    new_term_dict
                )
    current_file_terms_sorted = sorted(
        current_file_terms.items(), key=lambda kv: kv[0]
    )
    output_one_group(current_file_terms_sorted, output_file)


def keyfunc(line):
    return line.partition("\t")[0]


def main():
    for key, group in itertools.groupby(sys.stdin, keyfunc):
        reduce_one_group(key, group)


if __name__ == "__main__":
    main()
