#!/usr/bin/env python3
import sys

for line in sys.stdin:
    try:
        parts = line.strip().split('\t')
        if len(parts) == 4:
            word, doc_id, count, length = parts
            print(f"{word}\t{doc_id}\t{count}\t{length}")
        elif len(parts) == 3:
            word, df, index = parts
            print(f"{word}\t{df}\t{index}")
    except Exception as e:
        print(f"Error in mapper2: {str(e)}", file=sys.stderr)
        continue
