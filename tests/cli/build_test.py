import glob
import os
import shutil
import zipfile

import mock
import pytest

from pyleus import __version__
from pyleus import exception
from pyleus.cli import build


class TestBuild(object):

    @mock.patch.object(os.path, 'exists', autospec=True)
    def test__open_jar_jarfile_not_found(self, mock_exists):
        mock_exists.return_value = False
        with pytest.raises(exception.JarError):
            build._open_jar("foo")
        mock_exists.assert_called_once_with("foo")

    @mock.patch.object(os.path, 'exists', autospec=True)
    @mock.patch.object(zipfile, 'is_zipfile', autospec=True)
    def test__open_jar_not_jarfile(self, mock_is_zipfile, mock_exists):
        mock_exists.return_value = True
        mock_is_zipfile.return_value = False
        with pytest.raises(exception.JarError):
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
    def test__validate_venv_dir_contains_venv(self, mock_exists):
        mock_exists.return_value = True
        with pytest.raises(exception.InvalidTopologyError):
                build._validate_venv("foo", "foo/bar_venv")
        mock_exists.assert_called_once_with("foo/bar_venv")

    @mock.patch.object(build, '_remove_pyleus_base_jar', autospec=True)
    @mock.patch.object(build, 'VirtualenvProxy', autospec=True)
    def test__set_up_virtualenv_with_requirements(self, MockVenv, _):
        venv = MockVenv.return_value
        build._set_up_virtualenv(
            venv_name="foo",
            tmp_dir="bar",
            req="baz.txt",
            include_packages=["fruit", "ninja==7.7.7"],
            system_site_packages=True,
            pypi_index_url="http://pypi-ninja.ninjacorp.com/simple",
            verbose=False)
        expected_install = [
            mock.call("pyleus=={0}".format(__version__)),
            mock.call("fruit"),
            mock.call("ninja==7.7.7")
        ]
        venv.install_package.assert_has_calls(expected_install)
        venv.install_from_requirements.assert_called_once_with("baz.txt")

    @mock.patch.object(build, '_remove_pyleus_base_jar', autospec=True)
    @mock.patch.object(build, 'VirtualenvProxy', autospec=True)
    def test__set_up_virtualenv_without_requirements(self, MockVenv, _):
        venv = MockVenv.return_value
        build._set_up_virtualenv(
            venv_name="foo",
            tmp_dir="bar",
            req=None,
            include_packages=["fruit", "ninja==7.7.7"],
            system_site_packages=True,
            pypi_index_url="http://pypi-ninja.ninjacorp.com/simple",
            verbose=False)
        expected_install = [
            mock.call("pyleus=={0}".format(__version__)),
            mock.call("fruit"),
            mock.call("ninja==7.7.7")
        ]
        venv.install_package.assert_has_calls(expected_install)
        assert venv.install_from_requirements.call_count == 0

    @mock.patch.object(glob, 'glob', autospec=True)
    def test__content_to_copy(self, mock_glob):
        mock_glob.return_value = ["foo/good1.mkv", "foo/good2.bat",
                                  "foo/bad1.txt", "foo/bad2.jar"]
        content = build._content_to_copy("foo", ["foo/bad1.txt",
                                                 "foo/bad2.jar"])
        mock_glob.assert_called_once_with("foo/*")
        assert content == set(["foo/good1.mkv", "foo/good2.bat"])

    @mock.patch.object(build, '_content_to_copy', autospec=True)
    @mock.patch.object(os.path, 'isdir', autospec=True)
    @mock.patch.object(shutil, 'copytree', autospec=True)
    @mock.patch.object(shutil, 'copy2', autospec=True)
    def test__copy_dir_content(
            self, mock_copy2, mock_copytree, mock_isdir, mock_cont_to_copy):
        mock_cont_to_copy.return_value = ["foo/ham", "foo/honey"]
        mock_isdir.side_effect = iter([True, False])
        build._copy_dir_content(src="foo", dst="bar", exclude=[])
        mock_cont_to_copy.assert_called_once_with("foo", [])
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
