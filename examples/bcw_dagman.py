#!/usr/bin/env python
"""
Basic Calibration Workflow

Please see the README file in this directory for help.
"""
import os
import sys
import tempfile
import time

from spread.client import async_call




CODE_ROOT = '/usr/local/scratch'
DATA_ROOT = '/usr/local/scratch/data/dataset_001'
WORK_ROOT = '/tmp'

PROC_MEF = os.path.join(CODE_ROOT, 'bin', 'processMef.py')
PROC_SIF = os.path.join(CODE_ROOT, 'bin', 'processSif.py')
FINISH_MEF = os.path.join(CODE_ROOT, 'bin', 'finishMef.py')
CMD = '%s -i %s -o %s'
DATASET = 'raw-000001'
NUM_CCDS = len([l for l in open(os.path.join(DATA_ROOT, DATASET + '.fits')).readlines() if l.strip()])






def system(argv, **kwds):
    return(async_call('system', argv, kwds, fast=True))


def run(verbose=False):
    scratch_dir = tmpdir = tempfile.mkdtemp(dir=WORK_ROOT)

    if(verbose):
        print('Enqueueing PROC_MEF (time: %f)' % (time.time()))

    ifile = os.path.join(DATA_ROOT, '%s.fits' % (DATASET))
    ofile = '%s_' % (DATASET) + '%(ccdId)s.fits'
    defer = system(argv=[PROC_MEF, '-i', ifile, '-o', ofile],
                   cwd=scratch_dir, getenv=True)
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

        ifile = '%s_%d.fits' % (DATASET, _id)
        ofile = '%s_calib_%d.fits' % (DATASET, _id)
        defer = system(argv=[PROC_SIF, '-i', ifile, '-o', ofile],
                       cwd=res['cwd'], getenv=True)
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

    ifile = '%s_calib_' % (DATASET) + '%(ccdId)s.fits'
    ofile = '%s_calib.fits' % (DATASET)
    defer = system(argv=[FINISH_MEF, '-i', ifile, '-o', ofile, '-n', NUM_CCDS],
                   cwd=res['cwd'], getenv=True)
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

