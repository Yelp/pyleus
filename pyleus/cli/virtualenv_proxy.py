"""Simple interface to the basic functionalities offered by virtualenv and pip.

Options:
    system_site_packages - creating the virtualenv with this flag,
    pip will not download and install in the virtualenv all the
    dependencies already installed system-wide.
    index_url - allow to specify the URL of the Python Package Index.
    refer to the last pip install execution.
    verbose - if True all command will write on stdout.
"""
from __future__ import absolute_import

import os
import subprocess

from pyleus.exception import VirtualenvError


def _exec_shell_cmd(cmd, cwd, stdout, stderr, err_msg):
    """Interface to any bash command"""
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=stdout, stderr=stderr)
    out_data, _ = proc.communicate()
    if proc.returncode != 0:
        raise VirtualenvError(err_msg)
    return out_data


class VirtualenvProxy(object):
    """Object representing a ready-to-use virtualenv"""

    def __init__(self, name, path,
                 system_site_packages=False,
                 pypi_index_url=None,
                 verbose=False):
        """Creates the virtualenv with the options specified"""
        self._name = name
        self._path = path
        self._system_site_packages = system_site_packages
        self._pypi_index_url = pypi_index_url

        self._verbose = verbose
        self._out_stream = None
        if not self._verbose:
            self._out_stream = open(os.devnull, "w")
        self._err_stream = subprocess.STDOUT

        self._create_virtualenv()

    def _create_virtualenv(self):
        """Creates the actual virtualenv"""
        cmd = ["virtualenv", self._name]
        if self._system_site_packages:
            cmd.append("--system-site-packages")

        _exec_shell_cmd(cmd, cwd=self._path,
                        stdout=self._out_stream, stderr=self._err_stream,
                        err_msg="Failed to create virtualenv: {0}".
                        format(os.path.join(self._path, self._name)))

    def install_package(self, package):
        """Interface to `pip install SINGLE_PACKAGE`"""
        cmd = [os.path.join(self._name, "bin", "pip"), "install", package]

        if self._pypi_index_url is not None:
            cmd += ["-i", self._pypi_index_url]

        _exec_shell_cmd(
            cmd, cwd=self._path,
            stdout=self._out_stream, stderr=self._err_stream,
            err_msg="Failed to install {0} package."
            " Run with --verbose for detailed info.".format(package))

    def install_from_requirements(self, req):
        """Interface to `pip install -r REQUIREMENTS_FILE`"""
        cmd = [os.path.join(self._name, "bin", "pip"), "install", "-r", req]

        if self._pypi_index_url is not None:
            cmd += ["-i", self._pypi_index_url]

        _exec_shell_cmd(
            cmd, cwd=self._path,
            stdout=self._out_stream, stderr=self._err_stream,
            err_msg="Failed to install dependencies for this topology."
            " Run with --verbose for detailed info.")
