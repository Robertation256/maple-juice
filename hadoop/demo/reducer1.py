#!/usr/bin/env python3
import sys


current_value = None
current_count = 0


for line in sys.stdin:
    splitted = line.strip().split('\t')

    if len(splitted) == 1:
        detection = "EmptyString"
        count = int(splitted[0])
    else:
        detection = splitted[0]
        count = int(splitted[1])


    if current_value == detection:
        current_count += count
    else:
        if current_value:
            print(f"{current_value}\t{current_count}")
        current_value = detection
        current_count = count


if current_value:
    print(f"{current_value}\t{current_count}")
