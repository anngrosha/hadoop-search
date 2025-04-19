#!/usr/bin/env python3
import sys

current_word = None
current_df = 0

for line in sys.stdin:
    word, one = line.strip().split('\t')
    
    if word == current_word:
        current_df += 1
    else:
        if current_word:
            print(f"{current_word}\t{current_df}")
        current_word = word
        current_df = 1

if current_word:
    print(f"{current_word}\t{current_df}")
    