#!/usr/bin/env python3
########################################
# Map Reduce Implementation using pymp
# Jennifer Sanchez
# Automated Version
########################################
import sys
try:
    import pymp
except ImportError:
    print("PyMP Library cannot be loaded, is it installed?\n")
    sys.exit(1)

import os,time

def count(target, iterable):
    c = 0
    if not target in iterable:
        return 0
    for item in iterable:
        if item.lower() == target.lower():
            c += 1
    return c


if __name__ == '__main__':
    symbols = pymp.shared.list(['hate', 'love', 'death', 'night', 'sleep', 'time', 'henry', 'hamlet', 'you', 'my', 'blood', 'poison', 'macbeth', 'king', 'heart', 'honest'])
    candidates = [
        ['shakespeare1.txt', ''],
        ['shakespeare2.txt', ''],
        ['shakespeare3.txt', ''],
        ['shakespeare4.txt', ''],
        ['shakespeare5.txt', ''],
        ['shakespeare6.txt', ''],
        ['shakespeare7.txt', ''],
        ['shakespeare8.txt', ''],
    ]
    for source in candidates:
        with open(source[0], 'r') as f:
            source[1] = f.read()

    for nthreads in [1, 2, 4, 8]:
        start = time.time()
        t = 0
        with pymp.Parallel(int(nthreads)) as p:
            for symbol in p.iterate(symbols):
                for result in map(count, [symbol]*len(candidates), [x[1].split(' ') for x in candidates]):
                    t = (t + 1) % len(candidates)
        end = time.time()
        duration = end-start
        print("%d threads with Duration: %ss" % (nthreads, duration))
