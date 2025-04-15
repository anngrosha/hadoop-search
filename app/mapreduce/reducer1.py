#!/usr/bin/env python3
import sys
from collections import defaultdict

current_word = None
current_count = 0
doc_ids = []

for line in sys.stdin:
    word, doc_id, count, length = line.strip().split('\t')

    if not word or not doc_id or not count.isdigit() or not length.isdigit():
            print(f"Invalid record: {line}", file=sys.stderr)
            continue
    
    if word == current_word:
        current_count += 1
        doc_ids.append((doc_id, count, length))
    else:
        if current_word:
            print(f"{current_word}\t{current_count}\t{len(doc_ids)}")
            for doc in doc_ids:
                print(f"{current_word}\t{doc[0]}\t{doc[1]}\t{doc[2]}")
        current_word = word
        current_count = 1
        doc_ids = [(doc_id, count, length)]

if current_word:
    print(f"{current_word}\t{current_count}\t{len(doc_ids)}")
    for doc in doc_ids:
        print(f"{current_word}\t{doc[0]}\t{doc[1]}\t{doc[2]}")
