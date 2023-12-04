#!/usr/bin/env python3
import sys

sum = 0
detection_2_percentage = {}

for line in sys.stdin:
    splitted = line.strip().split('\t')

    if len(splitted) == 3:
        # Dummy_key value count
        detection = splitted[1]
        count = int(splitted[2])
        detection_2_percentage[detection] = count
        sum += count

for key in detection_2_percentage:
    percentage = detection_2_percentage[key] / sum * 100
    print(f"{key}\t{percentage}%")

    