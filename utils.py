import binascii
from datetime import datetime
from functools import wraps, update_wrapper
import logging
import os
import pudb
from subprocess import Popen, PIPE
import tempfile
import uuid

import boto
import pymysql


HOST = "dodata"
DO_CREDS_FILE = "/Users/ed/.docreds"
DB_CREDS_FILE = "/Users/ed/.dbcreds"
DB_NAME = "photoframe"
main_cursor = None
conn = None

LOG = logging.getLogger(__name__)


def trace():
    pudb.set_trace()


def runproc(cmd, decode=False):
    """
    Convenience method for executing operating system commands.

    Accepts a single string that would be the command as executed on the
    command line.

    Returns a 2-tuple consisting of the output of (STDOUT, STDERR). In your
    code you should check for an empty STDERR output to determine if your
    command completed successfully.
    """
    proc = Popen([cmd], shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE,
            close_fds=True)
    stdoutdata, stderrdata = proc.communicate()
    if decode:
        return (stdoutdata.decode(), stderrdata.decode())
    else:
        return (stdoutdata, stderrdata)


def _parse_creds():
    with open("/Users/ed/.dbcreds") as ff:
        lines = ff.read().splitlines()
    ret = {}
    for ln in lines:
        key, val = ln.split("=")
        ret[key] = val
    return ret


def connect():
    creds = _parse_creds()
    creds["DB_NAME"] = DB_NAME
    ret = pymysql.connect(host=HOST, user=creds["DB_USERNAME"],
            passwd=creds["DB_PWD"], db=creds["DB_NAME"], charset="utf8")
    return ret


def gen_uuid():
    return str(uuid.uuid4())


def get_cursor():
    global conn, main_cursor
    if not (conn and conn.open):
        LOG.debug("No DB connection")
        main_cursor = None
        conn = connect()
    if not main_cursor:
        LOG.debug("No cursor")
        main_cursor = conn.cursor(pymysql.cursors.DictCursor)
    return main_cursor


def commit():
    conn.commit()


def _user_creds():
    with open(DO_CREDS_FILE) as ff:
        creds = ff.read()
    user_creds = {}
    for ln in creds.splitlines():
        if ln.startswith("spacekey"):
            user_creds["spacekey"] = ln.split("=")[-1].strip()
        elif ln.startswith("secret"):
            user_creds["secret"] = ln.split("=")[-1].strip()
        elif ln.startswith("bucket"):
            user_creds["bucket"] = ln.split("=")[-1].strip()
    return user_creds


def create_client():
    user_creds = _user_creds()
    conn = boto.connect_s3(aws_access_key_id=user_creds["spacekey"],
            aws_secret_access_key=user_creds["secret"],
            host="nyc3.digitaloceanspaces.com")
    bucket = conn.get_bucket(user_creds["bucket"])
    return bucket


class SelfDeletingTempfile(object):
    """
    Convenience class for dealing with temporary files.

    The temp file is created in a secure fashion, and is automatically deleted
    when the context manager exits.

    Usage:

    \code
    with SelfDeletingTempfile() as tmp:
        tmp.write( ... )
        some_func(tmp)
    # More code
    # At this point, the tempfile has been erased.
    \endcode
    """
    name = None

    def __enter__(self):
        fd, self.name = tempfile.mkstemp()
        os.close(fd)
        return self.name

    def __exit__(self, type, value, traceback):
        os.unlink(self.name)
