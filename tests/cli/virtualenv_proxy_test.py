import __builtin__
import os
import subprocess

import mock
import testify as T

from pyleus import exception
from pyleus.cli import virtualenv_proxy
from pyleus.cli.virtualenv_proxy import VirtualenvProxy


VENV_NAME = "venv"
VENV_PATH = "/tmp/my/beloved"
PYPI_URL = "http://pypi-ninja.ninjacorp.com/simple"


class VirtualenvProxyTopLevelFunctionsTest(T.TestCase):

    @mock.patch.object(subprocess, 'Popen', autospec=True)
    def test__exec_bash_cmd(self, mock_popen):
        mock_proc = mock.Mock()
        mock_popen.return_value = mock_proc
        mock_proc.communicate.return_value = ["baz", "qux"]
        mock_proc.returncode = 1
        T.assert_raises_and_contains(
            exception.VirtualenvError, ["bar"],
            virtualenv_proxy._exec_shell_cmd, "bash_ninja",
            cwd="foo", stdout=42, stderr=666,
            err_msg="bar")
        mock_popen.assert_called_once_with(
            "bash_ninja", cwd="foo", stdout=42, stderr=666)
        mock_proc.communicate.assert_called_once_with()

    def test__strip_version(self):
        stripped_package = virtualenv_proxy._strip_version("Ninja==7.7.7")
        T.assert_equal(stripped_package, "Ninja")
        stripped_package = virtualenv_proxy._strip_version("Ninja>=7.7.7")
        T.assert_equal(stripped_package, "Ninja")
        stripped_package = virtualenv_proxy._strip_version("Ninja<=7.7.7")
        T.assert_equal(stripped_package, "Ninja")
        stripped_package = virtualenv_proxy._strip_version("Ninja")
        T.assert_equal(stripped_package, "Ninja")


class VirtualenvProxyCreationTest(T.TestCase):

    @mock.patch.object(__builtin__, 'open', autospec=True)
    @mock.patch.object(VirtualenvProxy, '_create_virtualenv', autospec=True)
    def test___init__(self, mock_create, mock_open):
        mock_open.return_value = 42
        venv = VirtualenvProxy(
            VENV_NAME,
            VENV_PATH,
            verbose=False)
        mock_open.assert_called_once_with(os.devnull, "w")
        T.assert_equal(venv._out_stream, 42)
        mock_create.assert_called_once_with(venv)

        venv = VirtualenvProxy(
            VENV_NAME,
            VENV_PATH,
            verbose=True)
        T.assert_equals(mock_open.call_count, 1)
        T.assert_equal(venv._out_stream, None)

    @mock.patch.object(__builtin__, 'open', autospec=True)
    @mock.patch.object(virtualenv_proxy, '_exec_shell_cmd', autospec=True)
    def test__create_virtualenv_system_site_packages(
            self, mock_cmd, mock_open):
        venv = VirtualenvProxy(
            VENV_NAME, VENV_PATH,
            system_site_packages=True, verbose=True)
        mock_cmd.assert_called_once_with(
            ["virtualenv", VENV_NAME, "--system-site-packages"],
            cwd=VENV_PATH,
            stdout=venv._out_stream,
            stderr=venv._err_stream,
            err_msg=mock.ANY
        )

    @mock.patch.object(__builtin__, 'open', autospec=True)
    @mock.patch.object(virtualenv_proxy, '_exec_shell_cmd', autospec=True)
    def test__create_virtualenv_no_system_site_packages(
            self, mock_cmd, mock_open):
        venv = VirtualenvProxy(
            VENV_NAME, VENV_PATH,
            system_site_packages=False, verbose=True)
        mock_cmd.assert_called_once_with(
            ["virtualenv", VENV_NAME],
            cwd=VENV_PATH,
            stdout=venv._out_stream,
            stderr=venv._err_stream,
            err_msg=mock.ANY
        )


class VirtualenvProxyMethodsTest(T.TestCase):

    @T.setup
    @mock.patch.object(__builtin__, 'open', autospec=True)
    @mock.patch.object(VirtualenvProxy, '_create_virtualenv', autospec=True)
    def setup_virtualenv(self, mock_create, mock_open):
        self.venv = VirtualenvProxy(
            VENV_NAME,
            VENV_PATH,
            pypi_index_url=PYPI_URL,
            verbose=False)

    @mock.patch.object(virtualenv_proxy, '_strip_version', autospec=True)
    @mock.patch.object(virtualenv_proxy, '_exec_shell_cmd', autospec=True)
    def test_is_package_installed_installed(self, mock_cmd, mock_strip):
        mock_cmd.return_value = "---\nName: ninja\n"
        installed = self.venv.is_package_installed("Ninja==7.7.7")
        mock_strip.assert_called_once_with("Ninja==7.7.7")
        mock_cmd.assert_called_once_with(
            ["{0}/bin/pip".format(VENV_NAME), "show", mock_strip.return_value],
            cwd=VENV_PATH,
            stdout=subprocess.PIPE,
            stderr=self.venv._out_stream,
            err_msg=mock.ANY)
        T.assert_equal(installed, True)

    @mock.patch.object(virtualenv_proxy, '_strip_version', autospec=True)
    @mock.patch.object(virtualenv_proxy, '_exec_shell_cmd', autospec=True)
    def test_is_package_installed_not_installed(self, mock_cmd, mock_strip):
        mock_cmd.return_value = ""
        installed = self.venv.is_package_installed("Ninja==7.7.7")
        mock_strip.assert_called_once_with("Ninja==7.7.7")
        mock_cmd.assert_called_once_with(
            ["{0}/bin/pip".format(VENV_NAME), "show", mock_strip.return_value],
            cwd=VENV_PATH,
            stdout=subprocess.PIPE,
            stderr=self.venv._out_stream,
            err_msg=mock.ANY)
        T.assert_equal(installed, False)

    @mock.patch.object(virtualenv_proxy, '_exec_shell_cmd', autospec=True)
    def test_install_package(self, mock_cmd):
        self.venv.install_package("Ninja==7.7.7")
        mock_cmd.assert_called_once_with(
            [
                "{0}/bin/pip".format(VENV_NAME), "install", "Ninja==7.7.7",
                "-i", PYPI_URL,
            ],
            cwd=VENV_PATH,
            stdout=self.venv._out_stream,
            stderr=self.venv._err_stream,
            err_msg=mock.ANY
        )

    @mock.patch.object(virtualenv_proxy, '_exec_shell_cmd', autospec=True)
    def test_install_from_requirements(self, mock_cmd):
        self.venv.install_from_requirements("foo.txt")
        mock_cmd.assert_called_once_with(
            [
                "{0}/bin/pip".format(VENV_NAME), "install",
                "-r", "foo.txt",
                "-i", PYPI_URL,
            ],
            cwd=VENV_PATH,
            stdout=self.venv._out_stream,
            stderr=self.venv._err_stream,
            err_msg=mock.ANY
        )


if __name__ == '__main__':
        T.run()
