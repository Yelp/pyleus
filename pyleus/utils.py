"""Utilities and helper functions."""
from __future__ import absolute_import

import os


def expand_path(path):
    """Return the corresponding absolute path after variables expansion."""
    return os.path.abspath(os.path.expanduser(path))
