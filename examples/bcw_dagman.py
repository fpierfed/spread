#!/usr/bin/env python
import os
import sys
import tempfile
import time

from spread.client import async_call




ROOT = '/usr/local/scratch'
PROC_MEF = os.path.join(ROOT, 'bin', 'processMef.py')
PROC_SIF = os.path.join(ROOT, 'bin', 'processSif.py')
FINISH_MEF = os.path.join(ROOT, 'bin', 'finishMef.py')
CMD = '%s -i %s -o %s'
DATASET = 'raw-000001'
DATA_ROOT = '/usr/local/scratch/data/dataset_001'
NUM_CCDS = len([l for l in open(os.path.join(DATA_ROOT, DATASET + '.fits')).readlines() if l.strip()])




def run(verbose=False):
    scratch_dir = tmpdir = tempfile.mkdtemp(dir='/tmp')

    if(verbose):
        print('Enqueueing PROC_MEF (time: %f)' % (time.time()))
    defer = async_call('system',
                       [PROC_MEF,
                        '-i',
                        os.path.join(DATA_ROOT, '%s.fits' % (DATASET)),
                        '-o',
                        '%s_' % (DATASET) + '%(ccdId)s.fits'],
                        {'cwd': scratch_dir,
                         'getenv': True},
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
                            '%s_%d.fits' % (DATASET, _id),
                            '-o',
                            '%s_calib_%d.fits' % (DATASET, _id)],
                           {'cwd': res['cwd'],
                            'getenv': True},
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
                         '%s_calib_' % (DATASET) + '%(ccdId)s.fits',
                        '-o',
                        '%s_calib.fits' % (DATASET),
                        '-n',
                        NUM_CCDS],
                       {'cwd': res['cwd'],
                        'getenv': True},
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

    print('BCW terminated normally. Results in %s' % (scratch_dir))
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

