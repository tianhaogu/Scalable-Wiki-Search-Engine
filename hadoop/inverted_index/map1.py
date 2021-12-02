#!/usr/bin/env python3
"""

Map 1.
Assign each single word the document id.
"""
import sys
import csv
import re


csv.field_size_limit(sys.maxsize)

STOP_WORDS = set()
stop_txt = "stopwords.txt"

with open(stop_txt, 'r') as stopfile:
    for line in stopfile:
        line = line.strip().casefold()
        STOP_WORDS.add(line)


def parse_text(doc_id, doc_title, doc_body):
    doc_text = doc_title + " " + doc_body
    doc_text = re.sub(r"[^a-zA-Z0-9 ]+", "", doc_text)
    doc_text = doc_text.casefold()
    doc_text_list = doc_text.split()
    for text in doc_text_list:
        if text != '' and text not in STOP_WORDS:
            print(f"{text}\t{doc_id}")


for line in sys.stdin:
    # doc_id = line.split(",")[0]
    # doc_id = re.sub(r"[^a-zA-Z0-9 ]+", "", doc_id)
    # doc_id = int(doc_id)
    # doc_title = line.split(",")[1]
    # doc_body = line.split(",")[2]
    # parse_text(doc_id, doc_title, doc_body)
    doc_raw = csv.reader([line])
    doc_info = list(doc_raw)
    doc_id = doc_info[0][0]
    doc_title = doc_info[0][1]
    doc_body = doc_info[0][2]
    parse_text(doc_id, doc_title, doc_body)
