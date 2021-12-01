#!/usr/bin/env python3
"""

Reduce 1.
term1  {doc_id_1: freq_1, doc_id_2: freq_2, ...}.
term2  {doc_id_1: freq_1, doc_id_2: freq_2, ...}.
Note that term is unique.
"""
import sys
import json
import itertools


def reduce_one_group(key, group):
    term_freq_per_term = {}
    for line in group:
        doc_id = line.partition("\t")[2]
        doc_id = doc_id.strip()
        if doc_id in term_freq_per_term:
            term_freq_per_term[doc_id] += 1
        else:
            term_freq_per_term[doc_id] = 1
    term_freq_per_term_obj = json.dumps(term_freq_per_term)
    print(f"{key}\t{term_freq_per_term_obj}")


def keyfunc(line):
    return line.partition("\t")[0]


def main():
    for key, group in itertools.groupby(sys.stdin, keyfunc):
        reduce_one_group(key, group)


if __name__ == "__main__":
    main()
