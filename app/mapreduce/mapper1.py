#!/usr/bin/env python3
import sys
import re
from collections import defaultdict

def tokenize(text):
    return re.findall(r'\w+', text.lower())

for line in sys.stdin:
    try:
        doc_id, title, text = line.strip().split('\t')
        words = tokenize(text)
        word_counts = defaultdict(int)
        
        for word in words:
            word_counts[word] += 1
            
        for word, count in word_counts.items():
            print(f"{word}\t{doc_id}\t{count}\t{len(words)}")
            
    except Exception as e:
        print(f"Error in mapper1: {str(e)}", file=sys.stderr)
        continue