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
        curr_count = int(line.split("\t")[1])
        doc_count += curr_count
    print(doc_count)
    # output_dir = Path.cwd() / "output0"
    # output_dir.mkdir(exist_ok=True)
    # output_file = output_dir / "part-00000"
    # output_file.touch()
    # doc_count = 0
    # for line in sys.stdin:
    #     curr_count = int(line.partition("\t")[2])
    #     doc_count += curr_count
    # with open(str(output_file), 'w', encoding='utf-8') as outfile:
    #     outfile.write(str(doc_count))
    # print(doc_count)


if __name__ == "__main__":
    main()
