#!/usr/bin/env python3
import sys

for line in sys.stdin:
    try:
        word, doc_id, count, length = line.strip().split('\t')
        print(f"{word}\t1")
    except Exception as e:
        print(f"Error in mapper2: {str(e)}", file=sys.stderr)
        continue