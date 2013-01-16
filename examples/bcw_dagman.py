#!/usr/bin/env python
import os
import sys

from client import async_call



ROOT = '/usr/local/scratch'
PROC_MEF = os.path.join(ROOT, 'bcw', 'processMef.py')
PROC_SIF = os.path.join(ROOT, 'bcw', 'processSif.py')
FINISH_MEF = os.path.join(ROOT, 'bcw', 'finishMef.py')
CMD = '%s -i %s -o %s'
DATASET = 'raw-000001'
NUM_CCDS = 4


print('Running PROC_MEF')
defer = async_call('system',
                   [PROC_MEF,
                    '-i',
                    os.path.join(ROOT, 'data/dataset_001/%s.fits' % (DATASET)),
                    '-o',
                    '/tmp/%s_' % (DATASET) + '%(ccdId)s.fits'])
while(not defer.is_ready()):
    defer.wait()
res = defer.result()
if(res['terminated'] or res['exit_code'] != 0):
    print(res)
    sys.exit(res['exit_code'])

results = []
defers = []
err = 0
for _id in range(NUM_CCDS):
    print('Running PROC_SIF')
    defer = async_call('system',
                       [PROC_SIF,
                        '-i',
                        '/tmp/%s_%d.fits' % (DATASET, _id),
                        '-o',
                        '/tmp/%s_calib_%d.fits' % (DATASET, _id)])
    defers.append(defer)

while(len(results) < NUM_CCDS):
    for _id in range(NUM_CCDS):
        if(not defers[_id].is_ready()):
            defers[_id].wait()
            continue

        res = defers[_id].result()
        results.append((_id, res))
        if(res['terminated'] or res['exit_code'] != 0):
            print(_id, res)
            err = res['exit_code']
if(err):
    print(results)
    sys.exit(err)

print('Running FINISH_MEF')
defer = async_call('system',
                   [FINISH_MEF,
                    '-i',
                     '/tmp/%s_calib_' % (DATASET) + '%(ccdId)s.fits',
                    '-o',
                    '/tmp/%s_calib.fits' % (DATASET),
                    '-n',
                    NUM_CCDS])
while(not defer.is_ready()):
    defer.wait()
res = defer.result()
if(res['terminated'] or res['exit_code'] != 0):
    print(res)
    sys.exit(res['exit_code'])

