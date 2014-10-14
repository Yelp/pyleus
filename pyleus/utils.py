"""Utilities and helper functions."""
from __future__ import absolute_import

import os


def expand_path(path):
    """Return the corresponding absolute path after variables expansion."""
    return os.path.abspath(os.path.expanduser(path))


def search_storm_cmd_path():
    """Search for the storm command in PATH."""
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    for path in os.environ["PATH"].split(os.pathsep):
        path = path.strip('"')
        exe_file = os.path.join(path, "storm")
        if is_exe(exe_file):
            return exe_file

    return None
