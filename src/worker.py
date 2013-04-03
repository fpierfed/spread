#!/usr/bin/env python
import collections
import datetime
import functools
import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import pika




# Constants
HOSTNAME = socket.gethostname()
logging.basicConfig(level=logging.CRITICAL)
UPDATED_CLASSAD = '''JobState=Running
JobPid=%(pid)d
NumPids=1
JobStartDate=%(start_time)d
RemoteSysCpu=0.00
RemoteUserCpu=%(exec_time).02f
ImageSize=0.00'''
EXITED_CLASSAD = '''JobState=Exited
JobPid=%(pid)d
NumPids=1
JobStartDate=%(start_time)d
RemoteSysCpu=0.00
RemoteUserCpu=%(exec_time).02f
ImageSize=0.00
ExitReason=exited
ExitBySignal=%(terminated)s
ExitSignal=%(signal)s
ExitCode=%(exit_code)d
JobDuration=%(exec_time).02f'''



def _exec(argv, stdin_str, stdout_filename, stderr_filename, environment,
    getenv, cwd, timeout, kill_after):
    """
    System call with timeout. Return the process info dictionary. We assume we
    are already in the right directory and that argv is a list of strings.
    """
    # pylint: disable=E1101
    terminated_t = None
    res = {'exit_code': None, 'stdout': '', 'stderr': '', 'argv': argv,
           'hostname': HOSTNAME, 'start_time': None, 'exec_time': None,
           'terminated': False, 'signal': None, 'cwd': cwd, 'pid': None}

    stdin = None
    if(stdin_str):
        stdin = tempfile.SpooledTemporaryFile()
        stdin.write(stdin_str)
        stdin.seek(0)
    stdout_file = None
    if(stdout_filename):
        try:
            stdout_file = open(stdout_filename, 'w')
        except:
            pass
    stderr_file = None
    if(stderr_filename):
        try:
            stderr_file = open(stderr_filename, 'w')
        except:
            pass

    start_time = time.time()
    cmd_env = _setup_environment(getenv, environment)
    res['start_time'] = start_time
    proc = subprocess.Popen(argv,
                            stdout=stdout_file,
                            stderr=stderr_file,
                            stdin=stdin,
                            env=cmd_env,
                            cwd=cwd)
    if(stdin_str):
        stdin.close()

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

    res['pid'] = proc.pid
    res['exit_code'] = proc.returncode
    if(stdout_file):
        stdout_file.close()
    if(stderr_file):
        stderr_file.close()
    del(proc)
    return(res)

def system(argv, environment=None, getenv=True, timeout=600, kill_after=10,
    root_dir=None, cleanup_after_errors=True, cwd=None,
    pre_proc=None, update_proc=None, post_proc=None, update_interval=1,
    classad=None, output=None, error=None, input=None, retries=3):
    """
    Execute the command line command described by the `argv` list. It is assumed
    that `argv[0]` is the executable and `argv[1:]`, if non empty, is its
    argument list.

    `environment`, if not None, is a dictionary specify the env variables for
    the command to be executed.

    If `getenv` == True then the user environment is inherited by the command
    being executed. If `getenv` == True and environment is not None, then the
    user environment is agumented/overridden by key, val pairs in `environment`.

    If `timeout` is a positive integer, it is the amount of seconds after which
    the command, if has not exited yet, will be sent a SIGTERM.

    If `kill_after` is a positive integer, it is the it is the amount of seconds
    after `timeout` when the command, if has not exited yet, will be sent a
    SIGKILL.

    If `root_dir` is not None, it specify the root directory where a temporary
    directory will be created for the command to execute in. After the command
    has exited the temporary directory is removed (see also `cwd` and
    `cleanup_after_errors`).

    If `cleanup_after_errors` == True and the command is either killed or exits
    with a non 0 status, then the temporary directory (if it was created) is
    removed, it is kept around otherwise.

    `cwd`, if not None, is the path to the directory where the command is
    executed from. If defined, it overrides `root_dir` (meaning that no
    temporary directory is created) and `cleanup_after_errors` is ignored (and
    no directory or files are ever deleted). If both `cwd` and `root_dir` are
    None, then it is assumed `cwd` = os.getcwd().

    `pre_proc`, `post_proc` and `update_proc` if not None specify scripts to
    execute, just before, just after and every `update_interval` seconds while
    `argv` is running. Each of these three scripts, if defined, receives in
    STDIN the job `classad`. The `update_proc` script receives as commend-line
    argument a string describing how the commend exited. Valid values are
    (exit, remove, hold, evict). Right now we only support exit.

    `classad` if not None is the textual rapresentation of the full Condor
    ClassAd for the job being executed. If at least one of the three `pre_proc`,
    `post_proc` and `update_proc` is defined, then `classad` has to be defined.

    `inout`, `output` and `error`, if defined, are the names of text files to be
    used for stream redirection. These can ba absolute paths or relative paths.
    In the latter case, they are assumed to be relative to `cwd` or `work_dir`,
    whichever is defined.

    Typical use of the *_proc scripts is to create and update database entries
    relative to the job being executed, e.g. in a blackboard architecture. The
    output of these scripts is simply ignored, just like their exit code. One
    exception is the exit code of the `proc_job`: if non 0, the other *_proc
    scripts are not executed.

    Return a result dictionary describing the results of the command execution:
        {'exit_code':   <integer>,
         'stdout':      <str>,
         'stderr':      <str>,
         'argv':        <list>,
         'hostname':    <str>,
         'start_time':  <float>,
         'exec_time':   <float>,
         'terminated':  <bool>,
         'cwd':         <str>}
    """
    if(retries is None or retries < 0):
        retries = 0
    else:
        try:
            retries = int(retries)
        except:
            retries = 0

    # Stringify argv.
    argv = map(unicode, argv)

    # Create a temp work dir and cd into it, assuming cwd is None.
    here = os.getcwd()
    created_word_dir = False
    if(cwd):
        work_dir = cwd
    elif(root_dir):
        work_dir = _mkworkdir(root_dir)
        created_word_dir = True
    else:
        work_dir = here
    os.chdir(work_dir)

    if(input):
        input = os.path.abspath(input)
    else:
        input = None
    if(output):
        output = os.path.abspath(output)
    else:
        output = None
    if(error):
        error = os.path.abspath(error)
    else:
        error = None

    # Pre
    pre_res = {}
    if(pre_proc and classad):
        print(' [.] %s - running PRE job (%s)' \
            % (str(datetime.datetime.utcnow()), pre_proc))
        pre_res = _exec(argv=[pre_proc, ],
                        stdin_str=classad,
                        stdout_filename=None,
                        stderr_filename=None,
                        environment=environment,
                        getenv=getenv,
                        cwd=work_dir,
                        timeout=timeout,
                        kill_after=kill_after)
        print(' [.] %s - PRE DONE (exit code: %d)' \
            % (str(datetime.datetime.utcnow()), pre_res['exit_code']))

    # Proc
    print(' [.] %s - running PROC job (%s)' \
        %(str(datetime.datetime.utcnow()), ' '.join(argv)))
    proc_error = True
    while(retries >= 0 and proc_error):
        res = _exec(argv=argv,
                    stdin_str=None,
                    stdout_filename=output,
                    stderr_filename=error,
                    # stdout_filename=None,
                    # stderr_filename=None,
                    environment=environment,
                    getenv=getenv,
                    cwd=work_dir,
                    timeout=timeout,
                    kill_after=kill_after)
        proc_error = res['exit_code'] != 0
        if(proc_error):
            retries -= 1
            time.sleep(.1)
    print(' [.] %s - PROC DONE (exit code: %d)' \
        % (str(datetime.datetime.utcnow()), res['exit_code']))

    # Update
    # TODO: Implement update_proc

    # Post, only if pre_proc exited OK or was not defined. Also agument the
    # classad with process-related info.
    if(post_proc and pre_res.get('exit_code', 0) == 0 and classad):
        print(' [.] %s - running POST job (%s)' \
            %(str(datetime.datetime.utcnow()), post_proc))
        classad += EXITED_CLASSAD % res
        post_res = _exec(argv=[post_proc, 'exit'],
                         stdin_str=classad,
                         stdout_filename=None,
                         stderr_filename=None,
                         environment=environment,
                         getenv=getenv,
                         cwd=work_dir,
                         timeout=timeout,
                         kill_after=kill_after)
        print(' [.] %s - POST DONE (exit code: %d)' \
            % (str(datetime.datetime.utcnow()), post_res['exit_code']))

    # Cleanup after yourselves!
    os.chdir(here)
    failed = res['exit_code'] != 0 or res['terminated']

    if(not created_word_dir or not cleanup_after_errors):
        if(failed):
            msg = 'Process exited with errors/was terminated. Work ' + \
                  'directory %s not removed.' % (work_dir)
            print(msg)
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

    # print " [.] %s(%s)"  % (fn, ', '.join([unicode(arg) for arg in argv]))
    response = getattr(sys.modules[__name__], fn)(argv, **kwds)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                     props.correlation_id),
                     body=json.dumps(response))
    ch.basic_ack(delivery_tag = method.delivery_tag)



if(__name__ == '__main__'):
    try:
        broker_host = sys.argv[1]
    except:
        print(' [i] No broker hostname specified, using localhost.')
        print(' [i] You can specify a hostname for the message broker as ' + \
              'first argument to this')
        print(' [i] script e.g. ./worker.py machine.example.com')
        broker_host = 'localhost'

    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=broker_host))
    channel = connection.channel()
    channel.queue_declare(queue='rpc_queue')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(on_request, queue='rpc_queue')

    print " [x] Awaiting RPC requests"
    channel.start_consuming()
