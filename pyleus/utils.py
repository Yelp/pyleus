"""Utilities and helper functions."""

import os

def expand_path(path):
    """Return the corresponding absolute path after variables expansion."""
    return os.path.abspath(os.path.expanduser(path))
