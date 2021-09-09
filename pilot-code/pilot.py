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

    f = None
    try:
        f = open(filename, mode)
    except IOError as exc:
        logging.error("exception caught: %s", exc)
        # raise exc

    return f


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


if __name__ == '__main__':

    establish_logging()
    version = '1.0.0.0'
    logging.info("Pilot X version %s", version)
    logging.info("Python version %s", sys.version)
    logging.info('working directory: %s', os.getcwd())

    host = os.getenv('DASK_SCHEDULER_IP', None)
    if not host:
        logging.warning('failed to get scheduler IP')
    else:
        # execute user code
        try:
            logging.info('connecting to scheduler at %s', host)
            client = Client(host)
        except Exception as exc:
            logging.warning('caught exception: %s', exc)
        else:
            logging.info('user code has finished')

    logging.info('Pilot X has finished')
    shutdown()
    sys.exit(0)
