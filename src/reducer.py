
#!/usr/bin/env python3
import sys

k = None
total = 0

for line in sys.stdin:
    try:
        key, num = line.strip().split('\t', 1)
        num = int(num)
    except (ValueError, IndexError):
        continue

    if k is not None and key != k:
        print('{}\t{}'.format(k, total))
        total = 0

    k = key
    total += num

if k is not None:
    print('{}\t{}'.format(k, total))
