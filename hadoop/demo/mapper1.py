#!/usr/bin/env python3


import sys
import os
import socket


interconn_value = os.environ.get("interconn", "default") #default is the default value
sys.stderr.write("The value of interconn_value is: {}\n".format(interconn_value))


col_index_interconn = 10
col_index_detection = 9


for line in sys.stdin:
    columns = line.strip().split(',')
    if "Interconne" in columns:
        continue

    # sys.stderr.write("The value of columns is: {}(\n".format(columns[col_index_interconn]))
    # sys.stderr.write("The value of detection is: {}\n".format(columns[col_index_detection]))


    if columns[col_index_interconn] == str(interconn_value).strip():
        detection = columns[col_index_detection]
        if len(detection.strip()) == 0:
            detection = "'" + detection + "'"
        print(f"{detection}\t1")