#!/usr/bin/env python
import os
import sys
import time

from spread.client import async_call




ROOT = '/usr/local/scratch'
PROC_MEF = os.path.join(ROOT, 'bcw', 'processMef.py')
PROC_SIF = os.path.join(ROOT, 'bcw', 'processSif.py')
FINISH_MEF = os.path.join(ROOT, 'bcw', 'finishMef.py')
CMD = '%s -i %s -o %s'
DATASET = 'raw-000001'
NUM_CCDS = 4



def run(verbose=False):
    if(verbose):
        print('Enqueueing PROC_MEF (time: %f)' % (time.time()))
    defer = async_call('system',
                       [PROC_MEF,
                        '-i',
                        os.path.join(ROOT,
                                     'data/dataset_001/%s.fits' % (DATASET)),
                        '-o',
                        '/tmp/%s_' % (DATASET) + '%(ccdId)s.fits'],
                       fast=True)
    if(verbose):
        print('Enqueued %f' % (time.time()))

    while(not defer.is_ready()):
        defer.wait()
    res = defer.result()
    if(verbose):
        print(res)
    if(res['terminated'] or res['exit_code'] != 0):
        return(res['exit_code'])

    results = {}
    defers = []
    err = 0
    for _id in range(NUM_CCDS):
        if(verbose):
            print('Enqueueing PROC_SIF (time: %f)' % (time.time()))
        defer = async_call('system',
                           [PROC_SIF,
                            '-i',
                            '/tmp/%s_%d.fits' % (DATASET, _id),
                            '-o',
                            '/tmp/%s_calib_%d.fits' % (DATASET, _id)],
                           fast=True)
        if(verbose):
            print('Enqueued %f' % (time.time()))
        defers.append(defer)

    while(len(results.keys()) < NUM_CCDS):
        for _id in range(NUM_CCDS):
            if(not defers[_id].is_ready()):
                defers[_id].wait()
                continue

            res = defers[_id].result()
            results[_id] = res
            if(res['terminated'] or res['exit_code'] != 0):
                err = res['exit_code']
    if(verbose):
        print(results)
    if(err):
        return(err)

    if(verbose):
        print('Enqueueing FINISH_MEF (time: %f)' % (time.time()))
    defer = async_call('system',
                       [FINISH_MEF,
                        '-i',
                         '/tmp/%s_calib_' % (DATASET) + '%(ccdId)s.fits',
                        '-o',
                        '/tmp/%s_calib.fits' % (DATASET),
                        '-n',
                        NUM_CCDS],
                       fast=True)
    if(verbose):
        print('Enqueued %f' % (time.time()))

    while(not defer.is_ready()):
        defer.wait()
    res = defer.result()
    if(verbose):
        print(res)
    if(res['terminated'] or res['exit_code'] != 0):
        return(res['exit_code'])
    return(0)




if(__name__ == '__main__'):
    verbose = False
    if(len(sys.argv) == 2 and sys.argv[1] == '-v'):
        verbose = True

    t0 = time.time()
    err = run(verbose)
    t1 = time.time()

    if(verbose):
        print('Started:  %f' % (t0))
        print('Ended:    %f' % (t1))
        print('Duration: %f' % (t1 - t0))

    sys.exit(err)

