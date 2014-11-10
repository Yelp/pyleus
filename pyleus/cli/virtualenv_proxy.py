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


def _exec_shell_cmd(cmd, stdout, stderr, err_msg):
    """Execute a shell command, returning the output

    If the call has a non-zero return code, raise a VirtualenError with
    err_msg.
    """
    proc = subprocess.Popen(cmd, stdout=stdout, stderr=stderr)
    out_data, _ = proc.communicate()
    if proc.returncode != 0:
        raise VirtualenvError(err_msg)
    return out_data


class VirtualenvProxy(object):
    """Object representing a ready-to-use virtualenv"""

    def __init__(self, path,
                 system_site_packages=False,
                 pypi_index_url=None,
                 use_wheel=True,
                 python_interpreter=None,
                 verbose=False):
        """Creates the virtualenv with the options specified"""
        self.path = path
        self._system_site_packages = system_site_packages
        self._pypi_index_url = pypi_index_url
        self._use_wheel = use_wheel
        self._python_interpreter = python_interpreter

        self._verbose = verbose
        self._out_stream = None
        if not self._verbose:
            self._out_stream = open(os.devnull, "w")
        self._err_stream = subprocess.STDOUT

        self._create_virtualenv()

    def _create_virtualenv(self):
        """Creates the actual virtualenv"""
        cmd = ["virtualenv", self.path]
        if self._system_site_packages:
            cmd.append("--system-site-packages")

        if self._python_interpreter:
            cmd.extend(["--python", self._python_interpreter])

        _exec_shell_cmd(cmd,
                        stdout=self._out_stream, stderr=self._err_stream,
                        err_msg="Failed to create virtualenv: {0}".
                                format(self.path))

    def install_package(self, package):
        """Interface to `pip install SINGLE_PACKAGE`"""
        cmd = [os.path.join(self.path, "bin", "pip"), "install", package]

        if self._pypi_index_url is not None:
            cmd += ["-i", self._pypi_index_url]

        if self._use_wheel:
            cmd += ['--use-wheel']

        _exec_shell_cmd(
            cmd, stdout=self._out_stream, stderr=self._err_stream,
            err_msg="Failed to install {0} package."
            " Run with --verbose for detailed info.".format(package))

    def install_from_requirements(self, req):
        """Interface to `pip install -r REQUIREMENTS_FILE`"""
        cmd = [os.path.join(self.path, "bin", "pip"), "install", "-r", req]

        if self._pypi_index_url is not None:
            cmd += ["-i", self._pypi_index_url]

        if self._use_wheel:
            cmd += ['--use-wheel']

        _exec_shell_cmd(
            cmd, stdout=self._out_stream, stderr=self._err_stream,
            err_msg="Failed to install dependencies for this topology."
            " Run with --verbose for detailed info.")

    def execute_module(self, module, args=None, cwd=None):
        """Call "virtualenv/interpreter -m" to execute a python module."""
        cmd = [os.path.join(self.path, "bin", "python"), "-m", module]

        if args:
            cmd += args

        proc = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                cwd=cwd)
        out_data, err_data = proc.communicate()
        if proc.returncode != 0:
            raise VirtualenvError("Failed to execute Python module: {0}."
                                  " Error: {1}".format(module, err_data))
        return out_data
