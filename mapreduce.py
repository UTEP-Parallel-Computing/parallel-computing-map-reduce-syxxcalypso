#!/usr/bin/env python3
########################################
# Map Reduce Implementation using pymp
# Jennifer Sanchez
# Manual Version
########################################
import sys
try:
    import pymp
except ImportError:
    print("PyMP Library cannot be loaded, is it installed?\n")
    sys.exit(1)

import os,time

try:
    nthreads = sys.argv[1]
except IndexError:
    print("Usage: python3 iDoTheStuff.py <thread_count>")
    sys.exit(0)

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
    start = time.time()
    c = 0
    with pymp.Parallel(int(nthreads)) as p:
        for symbol in p.iterate(symbols):
            total = 0
            for result in map(count, [symbol]*len(candidates), [x[1].split(' ') for x in candidates]):
                p.print("Found %3d occurrence(s) of %s in candidate %s" % (result, symbol, candidates[c][0]))
                c = (c + 1) % len(candidates)
                total += result
            p.print("Total occurrences of %s: %d\n" % (symbol, total))
    end = time.time()
    duration = end-start
    print("Duration: %ss" % duration)
