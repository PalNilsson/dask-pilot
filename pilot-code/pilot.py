# !/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2021

import os
import sys
import subprocess
import logging
import time
from dask.distributed import Client

# pilot x version
VERSION = "1.0.0.2"

# error codes
ERROR_UNSET_ENVVAR = 1
ERROR_SCHEDULER = 2
ERROR_JOBDEF = 3


def establish_logging(debug=True, nopilotlog=False, filename="pilotx.stdout", loglevel=0):
    """
    Setup and establish logging.

    Option loglevel can be used to decide which (predetermined) logging format to use.
    Example:
      loglevel=0: '%(asctime)s | %(levelname)-8s | %(name)-32s | %(funcName)-25s | %(message)s'
      loglevel=1: 'ts=%(asctime)s level=%(levelname)-8s event=%(name)-32s.%(funcName)-25s msg="%(message)s"'

    :param debug: debug mode (Boolean).
    :param nopilotlog: True when pilot log is not known (Boolean).
    :param filename: name of log file (string).
    :param loglevel: selector for logging level (int).
    :return:
    """

    _logger = logging.getLogger('')
    _logger.handlers = []
    _logger.propagate = False

    console = logging.StreamHandler(sys.stdout)
    if debug:
        format_str = '%(asctime)s | %(levelname)-8s | %(name)-32s | %(funcName)-25s | %(message)s'
        level = logging.DEBUG
    else:
        format_str = '%(asctime)s | %(levelname)-8s | %(message)s'
        level = logging.INFO

    if nopilotlog:
        logging.basicConfig(level=level, format=format_str, filemode='w')
    else:
        logging.basicConfig(filename=filename, level=level, format=format_str, filemode='w')
    console.setLevel(level)
    console.setFormatter(logging.Formatter(format_str))
    logging.Formatter.converter = time.gmtime
    _logger.addHandler(console)


def execute(cmd, mute=True):
    """
    Execute the given command with subprocess.Popen() and return the output.

    :param cmd: command (string).
    :param mute: Mute output if True (Boolean).
    :return: stdout (string), stderr (string).
    """
    _cmd = cmd.split(' ')
    process = subprocess.Popen(_cmd,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)

    stdout, stderr = process.communicate()
    stdout = stdout.decode('utf-8')
    stderr = stderr.decode('utf-8')
    if not mute:
        logging.info('cmd=%s', cmd)
        logging.info('stdout=\n%s', stdout)
        logging.info('stderr=\n%s', stderr)

    return stdout, stderr


def shutdown():
    """
    Shutdown logging.
    """

    logging.handlers = []
    logging.shutdown()


def open_file(filename, mode):
    """
    Open and return a file pointer for the given mode.
    Note: the caller needs to close the file.

    :param filename: file name (string).
    :param mode: file mode (character).
    :raises PilotException: FileHandlingFailure.
    :return: file pointer.
    """

    _file = ""
    try:
        _file = open(filename, mode)
    except IOError as exc:
        logging.error("exception caught: %s", exc)
        # raise exc

    return _file


def write_file(path, contents, mute=True, mode='w', unique=False):
    """
    Write given content to file.

    :param path: File path (string).
    :param contents: file contents (string).
    :param mute: Mute output (Boolean).
    :param mode: Write mode (char).
    :param unique: (not implemented).
    :return: status (Boolean).
    """
    status = False

    # add an incremental file name (add -%d if path already exists) if necessary
    #if unique:
    #    path = get_nonexistant_path(path)

    f = open_file(path, mode)
    if f:
        try:
            f.write(contents)
        except IOError as exc:
            #raise exc
            logging.error('caught exception: %s', exc)
        else:
            status = True
        f.close()

    if not mute and status:
        if 'w' in mode:
            logging.info('created file: %s', path)
        if 'a' in mode:
            logging.info('appended file: %s', path)

    return status


def get_job_definition_dict(job_id, shared_dir):
    """
    Locate the job definition in the shared directory matching the given job id.

    :param job_id: PanDA job id (string).
    :param shared_dir: shared directory path (string).
    :return: job_definition_dict (dict).
    """

    job_definition_dict = {}

    path = os.path.join(shared_dir, "job_definition-%s.json" % job_id)
    if os.path.exists(path):
        # read json
        pass
    else:
        logging.warning('file does not exist: %s', path)

    return job_definition_dict


def get_required_vars_dict():
    """
    Create a dictionary with variables that are required.

    The environment on the dask cluster expects the following env variables to be set
    1. DASK_SCHEDULER_IP: the Dask scheduler IP (e.g. tcp://127.0.0.1:8786)
    2. DASK_SHARED_FILESYSTEM_PATH: path to a shared directory used for pod communication, e.g. /mnt/dask
    3. PANDA_ID: a PanDA job id, e.g. 123456789 (set it to e.g. this number for local testing)

    The function returns a dictionary with the following format:
       required_vars = {
                        'host': $DASK_SCHEDULER_IP,
                        'shared_disk': $DASK_SHARED_FILESYSTEM_PATH,
                        'job_id': 'PANDA_ID'
                        }
    Keys and values will only be added if values are set.

    :return: required_vars (Dictionary)
    """

    required_vars = {}
    vars = {'host': 'DASK_SCHEDULER_IP', 'shared_disk': 'DASK_SHARED_FILESYSTEM_PATH', 'job_id': 'PANDA_ID'}
    for var in vars:
        _value = os.environ.get(vars[var], None)
        if _value:
            required_vars[var] = _value

    required_vars['url'] = os.getenv('DASK_USERCODE_URL', 'http://cern.ch/atlas-panda-pilot/dask_script.py')

    return required_vars


def setenv(vars):
    """
    Set environmental variables to the fields in the given dictionary.

    Format: { 'field1': value1, .. }
    ->
    os.environ['field1'] = value1
    ..

    :param vars: environment fields and values (dictionary).
    :return:
    """

    for var in vars:
        os.environ[var] = vars[var]


def exit(exit_code):
    """
    Exit the pilot.

    :param exit_code: exit code (integer).
    :return:
    """

    logging.info('Pilot X has finished with exit code %d', exit_code)
    shutdown()
    sys.exit(exit_code)


if __name__ == '__main__':

    establish_logging()
    logging.info("Pilot X version %s", VERSION)
    logging.info("Python version %s", sys.version)
    logging.info('working directory: %s', os.getcwd())

    exit_code = 0

    # get env vars
    required_vars = get_required_vars_dict()
    host = required_vars.get('host')
    url = required_vars.get('url')
    job_id = required_vars.get('job_id')
    shared_dir = required_vars.get('shared_dir')
    if not host or not url or not job_id or not shared_dir:
        logging.warning('failed to get scheduler IP (%s) / user code URL (%s) / job id (%s) / shared dir (%s)',
                        str(host), str(url), str(job_id), str(shared_dir))
        exit(ERROR_UNSET_ENVVAR)
    else:
        logging.warning('scheduler IP (%s) / user code URL (%s) / job id (%s) / shared dir (%s)',
                        str(host), str(url), str(job_id), str(shared_dir))
        logging.info('connecting to scheduler at %s', host)
        client = Client(host)
        if not client:
            logging.warning('failed to get dask client')
            exit(ERROR_SCHEDULER)

        # get job definition matching PanDA job id (should be set in env var)
        #job_definition = get_job_definition_dict(job_id, shared_dir)
        #if not job_definition:
        #    exit(ERROR_JOBDEF)

        # stage-in any input
        # (pass file list + metadata to stage-in/out pod "stageiox")
        # stage-in/out pod should be launched before pilot pod
        # pilot and stage-in/out pods communicate via the shared file system

        # get user code
        # (should later be in the job definition)
        stdout, stderr = execute("wget %s" % url)

        # set up required environment variables
        vars = {'DASK_SCHEDULER_IP': host,
                'DASK_SHARED_FILESYSTEM_PATH': shared_dir,
                'PANDA_ID': job_id}
        setenv(vars)

        # wait for stage-in pod to finish
        # ..

        # wait for workers to be ready
        
        try:
            # execute user code when stage-in pod is done
            # execute("python3 %s" % dask_script)
            pass
        except Exception as exc:
            logging.warning('caught exception while executing user code: %s', exc)
        else:
            logging.info('user code has finished')

            # stage-out output
            # (pass file list + metadata to stage-in/out pod "stageiox")

    exit(0)
