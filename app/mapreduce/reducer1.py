#!/usr/bin/env python3
import sys

current_word = None
documents = []

for line in sys.stdin:

    parts = line.strip().split('\t')

    if parts[0] == "!META!":
        print(f"{parts[1]}\t{parts[2]}\t{parts[3]}")
        continue

    word, doc_id, count, length = line.strip().split('\t')
    
    if word == current_word:
        documents.append((doc_id, count, length))
    else:
        if current_word:
            for doc in documents:
                print(f"{current_word}\t{doc[0]}\t{doc[1]}\t{doc[2]}")
        current_word = word
        documents = [(doc_id, count, length)]

if current_word:
    for doc in documents:
        print(f"{current_word}\t{doc[0]}\t{doc[1]}\t{doc[2]}")
