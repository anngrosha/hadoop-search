#!/usr/bin/env python3
import sys
import re

def tokenize(text):
    return re.findall(r'\w+', text.lower())

for line in sys.stdin:
    try:
        doc_id, title, text = line.strip().split('\t')
        words = tokenize(text)
        
        for word in set(words):
            count = words.count(word)
            print(f"{word}\t{doc_id}\t{count}\t{len(words)}")

        print(f"!META!\t{doc_id}\t{title}\t{len(words)}")
            
    except Exception as e:
        print(f"Error in mapper1: {str(e)}", file=sys.stderr)
        continue
