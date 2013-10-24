import __builtin__
import glob
import os
import mock
import shutil
import subprocess
import zipfile

import testify as T

from pyleus import exception
import pyleus.cli.jarbuilder as jarbuilder


class JarbuilderTest(T.TestCase):

    @mock.patch.object(os.path, 'exists', autospec=True)
    def test__open_jar_jarfile_not_found(self, mock_exists):
        mock_exists.return_value = False
        with T.assert_raises(exception.JarError):
            jarbuilder._open_jar("foo")
        mock_exists.assert_called_once_with("foo")

    @mock.patch.object(os.path, 'exists', autospec=True)
    @mock.patch.object(zipfile, 'is_zipfile', autospec=True)
    def test__open_jar_not_jarfile(self, mock_is_zipfile, mock_exists):
        mock_exists.return_value = True
        mock_is_zipfile.return_value = False
        with T.assert_raises(exception.JarError):
            jarbuilder._open_jar("foo")
        mock_is_zipfile.assert_called_once_with("foo")

    @mock.patch.object(os.path, 'exists', autospec=True)
    def test__validate_dir_not_found(self, mock_exists):
        mock_exists.return_value = False
        with T.assert_raises(exception.TopologyError):
            jarbuilder._validate_dir("foo")
        mock_exists.assert_called_once_with("foo")

    @mock.patch.object(os.path, 'exists', autospec=True)
    @mock.patch.object(os.path, 'isdir', autospec=True)
    def test__validate_dir_not_a_directory(self, mock_isdir, mock_exists):
        mock_exists.return_value = True
        mock_isdir.return_value = False
        with T.assert_raises(exception.TopologyError):
                jarbuilder._validate_dir("foo")
        mock_isdir.assert_called_once_with("foo")

    @mock.patch.object(os.path, 'isfile', autospec=True)
    def test__validate_yaml_not_found(self, mock_isfile):
        mock_isfile.return_value = False
        with T.assert_raises(exception.InvalidTopologyError):
            jarbuilder._validate_yaml("foo")
        mock_isfile.assert_called_once_with("foo")

    @mock.patch.object(os.path, 'isfile', autospec=True)
    def test__validate_req_not_found(self, mock_isfile):
        mock_isfile.return_value = False
        with T.assert_raises(exception.InvalidTopologyError):
            jarbuilder._validate_req("foo")
        mock_isfile.assert_called_once_with("foo")

    @mock.patch.object(os.path, 'exists', autospec=True)
    def test__validate_venv_dir_contains_venv(self, mock_exists):
        mock_exists.return_value = True
        with T.assert_raises(exception.InvalidTopologyError):
                jarbuilder._validate_venv("foo", "foo/bar_venv")
        mock_exists.assert_called_once_with("foo/bar_venv")

    @mock.patch.object(jarbuilder, '_validate_dir', autospec=True)
    @mock.patch.object(jarbuilder, '_validate_yaml', autospec=True)
    def test__validate_topology_no_use_virtualenv(
            self, mock_valid_yaml, mock_valid_dir):
        topo_dir = "foo"
        yaml = "foo/bar.yaml"
        req = "foo/baz.txt"
        venv = "foo/qux"
        jarbuilder._validate_topology(topo_dir, yaml, req, venv, False)
        mock_valid_dir.assert_called_once_with(topo_dir)
        mock_valid_yaml.assert_called_once_with(yaml)

    @mock.patch.object(jarbuilder, '_validate_dir', autospec=True)
    @mock.patch.object(jarbuilder, '_validate_yaml', autospec=True)
    @mock.patch.object(jarbuilder, '_validate_req', autospec=True)
    @mock.patch.object(jarbuilder, '_validate_venv', autospec=True)
    def test__validate_topology_use_virtualenv(
            self, mock_valid_venv, mock_valid_req,
            mock_valid_yaml, mock_valid_dir):
        topo_dir = "foo"
        yaml = "foo/bar.yaml"
        req = "foo/baz.txt"
        venv = "foo/qux"
        jarbuilder._validate_topology(topo_dir, yaml, req, venv, True)
        mock_valid_dir.assert_called_once_with(topo_dir)
        mock_valid_yaml.assert_called_once_with(yaml)
        mock_valid_req.assert_called_once_with(req)
        mock_valid_venv.assert_called_once_with(topo_dir, venv)

    @mock.patch.object(subprocess, 'Popen', autospec=True)
    def test__call_dep_cmd(self, mock_popen):
        mock_proc = mock.Mock()
        mock_popen.return_value = mock_proc
        mock_proc.communicate.return_value = ["baz", "qux"]
        mock_proc.returncode = 1
        T.assert_raises_and_contains(
            exception.DependenciesError, ["bar"],
            jarbuilder._call_dep_cmd, "bash_ninja",
            cwd="foo", stdout=42, stderr=666,
            err_msg="bar")
        mock_popen.assert_called_once_with(
            "bash_ninja", cwd="foo", stdout=42, stderr=666)
        mock_proc.communicate.assert_called_once_with()

    @mock.patch.object(jarbuilder, '_call_dep_cmd', autospec=True)
    def test__is_package_installed_installed(self, mock_dep_call):
        mock_dep_call.return_value = "---\nName: pyleus\n"
        installed = jarbuilder._is_package_installed(
            "foo", "pyleus", err_stream=42)
        mock_dep_call.assert_called_once_with(
            ["pyleus_venv/bin/pip", "show", "pyleus"],
            cwd="foo", stdout=subprocess.PIPE, stderr=42, err_msg=mock.ANY)
        T.assert_equal(installed, True)

    @mock.patch.object(jarbuilder, '_call_dep_cmd', autospec=True)
    def test__is_package_installed_not_installed(self, mock_dep_call):
        mock_dep_call.return_value = ""
        installed = jarbuilder._is_package_installed(
            "foo", "pyleus", err_stream=42)
        mock_dep_call.assert_called_once_with(
            ["pyleus_venv/bin/pip", "show", "pyleus"],
            cwd="foo", stdout=subprocess.PIPE, stderr=42, err_msg=mock.ANY)
        T.assert_equal(installed, False)

    @mock.patch.object(jarbuilder, '_call_dep_cmd', autospec=True)
    @mock.patch.object(__builtin__, 'open', autospec=True)
    @mock.patch.object(jarbuilder, '_is_package_installed', autospec=True)
    def test__virtualenv_pip_install_all_options(
            self, mock_inst, mock_open, mock_dep_call):
        mock_dep_call.side_effect = iter([0, 0, 0])
        mock_open.return_value = 42
        mock_inst.return_value = False
        jarbuilder._virtualenv_pip_install(
            tmp_dir="foo",
            req="bar",
            system=True,
            pypi_index_url="http://pypi-ninja.ninjacorp.com/simple",
            pip_log="baz",
            verbose=False)
        expected = [
            mock.call(["virtualenv", "pyleus_venv", "--system-site-packages"],
                      cwd="foo", stdout=42, stderr=subprocess.STDOUT,
                      err_msg=mock.ANY),
            mock.call(["pyleus_venv/bin/pip", "install", "pyleus",
                       "-i", "http://pypi-ninja.ninjacorp.com/simple"],
                      cwd="foo", stdout=42, stderr=subprocess.STDOUT,
                      err_msg=mock.ANY),
            mock.call(["pyleus_venv/bin/pip", "install", "-r", "bar",
                       "-i", "http://pypi-ninja.ninjacorp.com/simple",
                       "--log", "baz"],
                      cwd="foo", stdout=42, stderr=subprocess.STDOUT,
                      err_msg=mock.ANY),
        ]
        mock_dep_call.assert_has_calls(expected)
        mock_open.assert_called_once_with(os.devnull, "w")
        mock_inst.assert_called_once_with("foo", "pyleus", err_stream=42)

    @mock.patch.object(glob, 'glob', autospec=True)
    def test__exclude_content(self, mock_glob):
        mock_glob.return_value = ["foo/spam", "foo/ham",
                                  "foo/requirements.txt",
                                  "foo/pyleus_topology.yaml"]
        content = jarbuilder._exclude_content("foo", True)
        mock_glob.assert_called_once_with("foo/*")
        T.assert_sorted_equal(content, ["foo/spam", "foo/ham"])

    @mock.patch.object(jarbuilder, '_exclude_content', autospec=True)
    @mock.patch.object(os.path, 'isdir', autospec=True)
    @mock.patch.object(shutil, 'copytree', autospec=True)
    @mock.patch.object(shutil, 'copy2', autospec=True)
    def test__copy_dir_content(
            self, mock_copy2, mock_copytree, mock_isdir, mock_exclude_cont):
        mock_exclude_cont.return_value = ["foo/ham", "foo/honey"]
        mock_isdir.side_effect = iter([True, False])
        jarbuilder._copy_dir_content("foo", "bar", True)
        mock_exclude_cont.assert_called_once_with("foo", True)
        expected = [mock.call("foo/ham"), mock.call("foo/honey")]
        mock_isdir.assert_has_calls(expected)
        mock_copytree.assert_called_once_with(
            "foo/ham", "bar/ham", symlinks=True)
        mock_copy2.assert_called_once_with("foo/honey", "bar")

    @mock.patch.object(os, 'walk', autospec=True)
    def test__zip_dir(self, mock_walk):
        mock_arc = mock.Mock(autospec=True)
        mock_walk.return_value = [
            ("foo", ["bar"], ["baz"]),
            ("foo/bar", [], ["qux"])
        ]
        jarbuilder._zip_dir("foo", mock_arc)
        mock_walk.assert_any_call("foo")
        expected = [
            mock.call("foo/baz", "baz", zipfile.ZIP_DEFLATED),
            mock.call("foo/bar/qux", "bar/qux", zipfile.ZIP_DEFLATED),
        ]
        mock_arc.write.assert_has_calls(expected)

    @mock.patch.object(os.path, 'exists', autospec=True)
    def test__pack_jar_output_jar_already_exists(self, mock_exists):
        mock_exists.return_value = True
        with T.assert_raises(exception.JarError):
            jarbuilder._pack_jar("foo", "bar")
        mock_exists.assert_called_once_with("bar")

    @mock.patch.object(os.path, 'exists', autospec=True)
    @mock.patch.object(zipfile, 'ZipFile', autospec=True)
    @mock.patch.object(jarbuilder, '_zip_dir', autospec=True)
    def test__pack_jar(self, mock_zip_dir, mock_zipfile, mock_exists):
        mock_exists.return_value = False
        jarbuilder._pack_jar("foo", "bar")
        mock_zipfile.assert_called_once_with("bar", "w")
        mock_zip_dir.assert_called_once_with("foo", mock_zipfile.return_value)

    @mock.patch.object(os.path, 'isfile', autospec=True)
    def test__is_virtualenv_required(self, mock_isfile):
        mock_configs = mock.Mock()
        mock_configs.use_virtualenv = None
        req = "foo/baz.txt"

        mock_isfile.return_value = False
        flag = jarbuilder._is_virtualenv_required(mock_configs, req)
        mock_isfile.assert_called_once_with(req)
        T.assert_equals(flag, False)

        mock_isfile.return_value = True
        flag = jarbuilder._is_virtualenv_required(mock_configs, req)
        T.assert_equals(flag, True)

        mock_configs.use_virtualenv = False
        flag = jarbuilder._is_virtualenv_required(mock_configs, req)
        T.assert_equals(flag, False)

        mock_configs.use_virtualenv = True
        mock_isfile.return_value = False
        flag = jarbuilder._is_virtualenv_required(mock_configs, req)
        T.assert_equals(flag, True)

    @mock.patch.object(os, 'path', autospec=True)
    def test__expand_path(self, mock_path):
        mock_path.abspath.return_value = "bar"
        expanded = jarbuilder._expand_path("foo")
        mock_path.abspath.assert_has_calls([
            mock.call(mock_path.expanduser("foo"))])
        T.assert_equals(expanded, "bar")

    @mock.patch.object(jarbuilder, '_expand_path', autospec=True)
    def test__build_otuput_path(self, mock_ex_path):
        jarbuilder._build_output_path("foo", "bar")
        mock_ex_path.assert_called_with("foo")

        jarbuilder._build_output_path(None, "bar")
        mock_ex_path.assert_called_with("bar.jar")

    @mock.patch.object(os.path, 'exists', autospec=True)
    def test__validate_config_file_not_found(
            self, mock_exists):
        mock_exists.return_value = False
        with T.assert_raises(exception.ConfigurationError):
            jarbuilder._validate_config_file("foo")
        mock_exists.assert_called_once_with("foo")

    @mock.patch.object(os.path, 'exists', autospec=True)
    @mock.patch.object(os.path, 'isfile', autospec=True)
    def test__validate_config_file_not_a_file(
            self, mock_isfile, mock_exists):
        mock_exists.return_value = True
        mock_isfile.return_value = False
        with T.assert_raises(exception.ConfigurationError):
            jarbuilder._validate_config_file("foo")
        mock_exists.assert_called_once_with("foo")
        mock_isfile.assert_called_once_with("foo")

    def test__update_configuration(self):
        default_config = jarbuilder.DEFAULTS
        update_dict = {
            "pypi_index_url": "http://pypi-ninja.ninjacorp.com/simple"}
        updated_config = jarbuilder._update_configuration(
            default_config, update_dict)
        T.assert_equal(default_config.pypi_index_url, None)
        T.assert_equal(updated_config.pypi_index_url,
                       "http://pypi-ninja.ninjacorp.com/simple")


if __name__ == '__main__':
        T.run()
