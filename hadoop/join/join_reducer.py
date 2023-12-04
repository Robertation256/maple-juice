#!/usr/bin/env python3


import sys
import os

dataset2Lines = {}

for line in sys.stdin:
    splitted = line.strip().split("\t")
    if len(splitted) == 3:
        dataset = splitted[1].strip()
        content = splitted[2].strip()
        if dataset in dataset2Lines:
            dataset2Lines[dataset].append(content)
        else:
            dataset2Lines[dataset] = [content]


k1, k2 = dataset2Lines
for i in dataset2Lines[k1]:
    for j in dataset2Lines[k2]:
        print(f"{i}\t{j}")