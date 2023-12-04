#!/usr/bin/env python3

import sys
import re

pattern = re.compile(r'Al')

for line in sys.stdin:
    line = line.strip()

    if pattern.search(line):
        print(f"1\t{line}")
