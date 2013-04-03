#!/usr/bin/env python
import logging
import multiprocessing
import os
import subprocess
import sys
import time





try:
    cmd = sys.argv[1]
except:
    print('Usage start_workers.pt <cmd> <options>')

try:
    N = int(sys.argv[2])
except:
    N = multiprocessing.cpu_count()


print('Starting %d instances of %s' % (N, cmd))
print('STDOUT and STDERR in workers.log')

procs = []
with open('workers.log', 'w') as f:
    for i in range(N):
        procs.append(subprocess.Popen([cmd, ],
                                      stdout=f,
                                      stderr=subprocess.STDOUT))

    failed = 0
    completed = 0
    try:
        while(procs):
            p = procs.pop(0)
            err = p.returncode
            if(err is None):
                procs.append(p)
            elif(err != 0):
                failed += 1
            else:
                completed += 1
            time.sleep(1.)
            f.flush()
    except KeyboardInterrupt:
        print('Killing all running processes')
        while(procs):
            p = procs.pop(0)
            p.kill()
        print('Done. Quitting.')

