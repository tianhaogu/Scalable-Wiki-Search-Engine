#!/usr/bin/env python3
"""

Map 0.
Assign each (unique doc_id) a value 1.
"""
import sys
import re


for line in sys.stdin:
    doc_fix = 1
    # doc_id = line.split(",")[0]
    # doc_id = re.sub(r"[^a-zA-Z0-9 ]+", "", doc_id)
    # doc_id = int(doc_id)
    print(f"{doc_fix}\t1")
