import glob
import os
import shutil
import zipfile

import mock
import testify as T

from pyleus import exception
from pyleus.cli import build

class BuildTest(T.TestCase):

    @mock.patch.object(os.path, 'exists', autospec=True)
    def test__open_jar_jarfile_not_found(self, mock_exists):
        mock_exists.return_value = False
        with T.assert_raises(exception.JarError):
            build._open_jar("foo")
        mock_exists.assert_called_once_with("foo")

    @mock.patch.object(os.path, 'exists', autospec=True)
    @mock.patch.object(zipfile, 'is_zipfile', autospec=True)
    def test__open_jar_not_jarfile(self, mock_is_zipfile, mock_exists):
        mock_exists.return_value = True
        mock_is_zipfile.return_value = False
        with T.assert_raises(exception.JarError):
            build._open_jar("foo")
        mock_is_zipfile.assert_called_once_with("foo")

    @mock.patch.object(os, 'walk', autospec=True)
    def test__zip_dir(self, mock_walk):
        mock_arc = mock.Mock(autospec=True)
        mock_walk.return_value = [
            ("foo", ["bar"], ["baz"]),
            ("foo/bar", [], ["qux"])
        ]
        build._zip_dir("foo", mock_arc)
        mock_walk.assert_any_call("foo")
        expected = [
            mock.call("foo/baz", "baz", zipfile.ZIP_DEFLATED),
            mock.call("foo/bar/qux", "bar/qux", zipfile.ZIP_DEFLATED),
        ]
        mock_arc.write.assert_has_calls(expected)

    @mock.patch.object(zipfile, 'ZipFile', autospec=True)
    @mock.patch.object(build, '_zip_dir', autospec=True)
    def test__pack_jar(self, mock_zip_dir, mock_zipfile):
        build._pack_jar("foo", "bar")
        mock_zipfile.assert_called_once_with("bar", "w")
        mock_zip_dir.assert_called_once_with("foo", mock_zipfile.return_value)

    @mock.patch.object(os.path, 'exists', autospec=True)
    def test__validate_dir_not_found(self, mock_exists):
        mock_exists.return_value = False
        with T.assert_raises(exception.TopologyError):
            build._validate_dir("foo")
        mock_exists.assert_called_once_with("foo")

    @mock.patch.object(os.path, 'exists', autospec=True)
    @mock.patch.object(os.path, 'isdir', autospec=True)
    def test__validate_dir_not_a_directory(self, mock_isdir, mock_exists):
        mock_exists.return_value = True
        mock_isdir.return_value = False
        with T.assert_raises(exception.TopologyError):
                build._validate_dir("foo")
        mock_isdir.assert_called_once_with("foo")

    @mock.patch.object(os.path, 'isfile', autospec=True)
    def test__validate_yaml_not_found(self, mock_isfile):
        mock_isfile.return_value = False
        with T.assert_raises(exception.InvalidTopologyError):
            build._validate_yaml("foo")
        mock_isfile.assert_called_once_with("foo")

    @mock.patch.object(os.path, 'isfile', autospec=True)
    def test__validate_req_not_found(self, mock_isfile):
        mock_isfile.return_value = False
        with T.assert_raises(exception.InvalidTopologyError):
            build._validate_req("foo")
        mock_isfile.assert_called_once_with("foo")

    @mock.patch.object(os.path, 'exists', autospec=True)
    def test__validate_venv_dir_contains_venv(self, mock_exists):
        mock_exists.return_value = True
        with T.assert_raises(exception.InvalidTopologyError):
                build._validate_venv("foo", "foo/bar_venv")
        mock_exists.assert_called_once_with("foo/bar_venv")

    @mock.patch.object(build, '_validate_dir', autospec=True)
    @mock.patch.object(build, '_validate_yaml', autospec=True)
    @mock.patch.object(build, '_validate_venv', autospec=True)
    @mock.patch.object(build, '_validate_req', autospec=True)
    def test__validate_topology_no_use_virtualenv(
            self, mock_valid_req, mock_valid_venv,
            mock_valid_yaml, mock_valid_dir):
        topo_dir = "foo"
        yaml = "foo/bar.yaml"
        req = "foo/baz.txt"
        venv = "foo/qux"
        build._validate_topology(topo_dir, yaml, req, venv, False)
        mock_valid_dir.assert_called_once_with(topo_dir)
        mock_valid_yaml.assert_called_once_with(yaml)
        mock_valid_venv.assert_called_once_with(topo_dir, venv)
        T.assert_equal(mock_valid_req.call_count, 0)

    @mock.patch.object(build, '_validate_dir', autospec=True)
    @mock.patch.object(build, '_validate_yaml', autospec=True)
    @mock.patch.object(build, '_validate_venv', autospec=True)
    @mock.patch.object(build, '_validate_req', autospec=True)
    def test__validate_topology_use_virtualenv(
            self, mock_valid_req, mock_valid_venv,
            mock_valid_yaml, mock_valid_dir):
        topo_dir = "foo"
        yaml = "foo/bar.yaml"
        req = "foo/baz.txt"
        venv = "foo/qux"
        build._validate_topology(topo_dir, yaml, req, venv, True)
        mock_valid_dir.assert_called_once_with(topo_dir)
        mock_valid_yaml.assert_called_once_with(yaml)
        mock_valid_venv.assert_called_once_with(topo_dir, venv)
        mock_valid_req.assert_called_once_with(req)

    @mock.patch.object(build, 'VirtualenvProxy', autospec=True)
    def test__set_up_virtualenv(self, MockVenv):
        venv = MockVenv.return_value
        venv.is_package_installed.side_effect = iter([False, True, False])
        build._set_up_virtualenv(
            venv_name="foo",
            tmp_dir="bar",
            req="baz.txt",
            include_packages=["fruit", "ninja==7.7.7"],
            system_site_packages=True,
            pypi_index_url="http://pypi-ninja.ninjacorp.com/simple",
            verbose=False)
        expected_is_installed = [
            mock.call("pyleus"),
            mock.call("fruit"),
            mock.call("ninja==7.7.7")
        ]
        expected_install = [
            mock.call("pyleus"),
            mock.call("ninja==7.7.7")
        ]
        venv.is_package_installed.assert_has_calls(expected_is_installed)
        venv.install_package.assert_has_calls(expected_install)
        venv.install_from_requirements.assert_called_once_with("baz.txt")

    @mock.patch.object(os.path, 'isfile', autospec=True)
    def test__is_virtualenv_required(self, mock_isfile):
        req = "foo/baz.txt"

        mock_isfile.return_value = False
        flag = build._is_virtualenv_required(req)
        mock_isfile.assert_called_once_with(req)
        T.assert_equals(flag, False)

        mock_isfile.return_value = True
        flag = build._is_virtualenv_required(req)
        T.assert_equals(flag, True)

    @mock.patch.object(glob, 'glob', autospec=True)
    def test__exclude_content(self, mock_glob):
        mock_glob.return_value = ["foo/spam", "foo/ham",
                                  "foo/requirements.txt",
                                  "foo/pyleus_topology.yaml"]
        content = build._exclude_content("foo", True)
        mock_glob.assert_called_once_with("foo/*")
        T.assert_sorted_equal(content, ["foo/spam", "foo/ham"])

    @mock.patch.object(build, '_exclude_content', autospec=True)
    @mock.patch.object(os.path, 'isdir', autospec=True)
    @mock.patch.object(shutil, 'copytree', autospec=True)
    @mock.patch.object(shutil, 'copy2', autospec=True)
    def test__copy_dir_content(
            self, mock_copy2, mock_copytree, mock_isdir, mock_exclude_cont):
        mock_exclude_cont.return_value = ["foo/ham", "foo/honey"]
        mock_isdir.side_effect = iter([True, False])
        build._copy_dir_content("foo", "bar", True)
        mock_exclude_cont.assert_called_once_with("foo", True)
        expected = [mock.call("foo/ham"), mock.call("foo/honey")]
        mock_isdir.assert_has_calls(expected)
        mock_copytree.assert_called_once_with(
            "foo/ham", "bar/ham", symlinks=True)
        mock_copy2.assert_called_once_with("foo/honey", "bar")

    @mock.patch.object(build, 'expand_path', autospec=True)
    def test__build_otuput_path(self, mock_ex_path):
        build._build_output_path("foo", "bar")
        mock_ex_path.assert_called_with("foo")

        build._build_output_path(None, "bar")
        mock_ex_path.assert_called_with("bar.jar")


if __name__ == '__main__':
        T.run()
