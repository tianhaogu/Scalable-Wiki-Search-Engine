#!/usr/bin/env python3
"""

Reduce 0.
Output the number of documents to the txt file.
"""
import sys
from pathlib import Path


def main():
    doc_count = 0
    for line in sys.stdin:
        curr_count = int(line.partition("\t")[2])
        doc_count += curr_count
    print(doc_count)


if __name__ == "__main__":
    main()
