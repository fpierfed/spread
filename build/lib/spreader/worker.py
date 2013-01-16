#!/usr/bin/env python
import collections
import functools
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

import pika




def _exec(argv, environment, getenv, cwd, timeout, kill_after):
    """
    System call with timeout. Return the process info dictionary. We assume we
    are already in the right directory and that argv is a list of strings.
    """
    # pylint: disable=E1101
    terminated_t = None
    res = {'exit_code': None, 'stdout': None, 'stderr': None, 'argv': argv,
           'hostname': 'localhost', 'start_time': None, 'exec_time': None,
           'terminated': False}

    start_time = time.time()
    res['start_time'] = start_time
    proc = subprocess.Popen(argv,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            env=_setup_environment(getenv, environment),
                            cwd=cwd)

    while(proc.poll() is None):
        if(timeout > 0 and time.time() - start_time >= timeout):
            res['terminated'] = True
            terminated_t = time.time()
            msg = '%s still executing after timeout of %s s. Sending SIGTERM'
            proc.terminate()
            break
        time.sleep(.1)
    res['exec_time'] = time.time() - start_time

    if(terminated_t is not None):
        if(proc.poll() is None):
            time.sleep(kill_after)
        if(proc.poll() is None):
            msg = '%s still executing after %s s since SIGTERM. Sending SIGKILL'
            proc.kill()

    res['exit_code'] = proc.returncode
    res['stdout'] = proc.stdout.read()
    proc.stdout.close()
    res['stderr'] = proc.stderr.read()
    proc.stderr.close()
    del(proc)
    return(res)

def system(argv, environment=None, getenv=False, timeout=600, kill_after=10,
    root_dir='/tmp', cleanup_after_errors=True, cwd=None):
    # Stringify argv.
    argv = map(unicode, argv)

    # Create a temp work dir and cd into it.
    here = os.getcwd()
    work_dir = _mkworkdir(root_dir)
    os.chdir(work_dir)

    res = _exec(argv, environment, getenv, cwd, timeout, kill_after)

    # Cleanup after yourselves!
    os.chdir(here)
    failed = res['exit_code'] != 0 or res['terminated']
    if(failed and not cleanup_after_errors):
        msg = 'Process exited with errors/was terminated. Work directory %s ' +\
              'not removed.'
    else:
        _rmworkdir(work_dir)
    return(res)

def _mkworkdir(root_dir):
    """
    Create the temporary work directory under root_dir. Return the absolute path
    to the created temporary directory.
    """
    # FIXME: create as unprivileged user.
    tmpdir = tempfile.mkdtemp(dir=root_dir)
    return(tmpdir)

def _rmworkdir(path):
    """
    Remove the temprary work directory and log any error which might occur. We
    otherwise do not act on these errors (apart form logging them), which means
    that the temporary directory might end up being partially left behind.
    """
    shutil.rmtree(path, onerror=_failed_del_warn)
    return

def _setup_environment(getenv, extra_env):
    """
    Setup the complete environment for process execution. Optionally (if
    getenv=True), inherit the current user environment. Optionally agument it
    using the extra_env dictionary.
    """
    # FIXME: what if getenv = False and environment is empty?
    full_env = {}
    if(not extra_env):
        extra_env = {}

    if(getenv):
        full_env = os.environ
    if(extra_env):
        full_env.update(extra_env)
    return(full_env)

def _failed_del_warn(function, path, excinfo):
    """
    Just log what happened (i.e. that we failed to remove a file or directory)
    and move on.
    """
    msg = 'Error in removing %s. The parent directory will not be removed.'
    print(msg % (path))

    print('Error in calling %s on %s.' % (function.__name__, path))
    print('Exception: %s %s' % (excinfo[0], excinfo[1]))
    print('Traceback: %s' % (excinfo[-1]))
    return

def on_request(ch, method, props, body):
    [fn, argv, kwds] = json.loads(body)

    print " [.] %s(%s)"  % (fn, ', '.join([unicode(arg) for arg in argv]))
    response = getattr(sys.modules[__name__], fn)(argv, **kwds)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                     props.correlation_id),
                     body=json.dumps(response))
    ch.basic_ack(delivery_tag = method.delivery_tag)



if(__name__ == '__main__'):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='rpc_queue')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(on_request, queue='rpc_queue')

    print " [x] Awaiting RPC requests"
    channel.start_consuming()
