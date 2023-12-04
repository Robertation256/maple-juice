#!/usr/bin/env python3


import sys
import os


d1_join_col = os.environ.get("d2_col", "default") #default is the default value
d1_join_col = int(d1_join_col)

for line in sys.stdin:
    columns = line.strip().split(',')

    if len(columns) > d1_join_col:
        join_value = columns[d1_join_col].strip()
        print(f"{join_value}\t{line.strip()}\td1")