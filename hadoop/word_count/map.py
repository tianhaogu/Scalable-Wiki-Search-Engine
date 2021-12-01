#!/usr/bin/env python3
"""Word count mapper."""
import sys
import csv
import re


for line in sys.stdin:
    words = line.split()
    for word in words:
        print(f"{word}\t1")