#!/usr/bin/env python3

import sys

for line in sys.stdin:
    line = line.strip()
    dummyKey, content = line.Split("\t")
    print(content)
