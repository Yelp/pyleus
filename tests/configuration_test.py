import os

import pytest

from pyleus import configuration
from pyleus import exception
from pyleus.testing import mock


class TestConfiguration(object):

    @mock.patch.object(os.path, 'exists', autospec=True)
    def test__validate_config_file_not_found(
            self, mock_exists):
        mock_exists.return_value = False
        with pytest.raises(exception.ConfigurationError):
            configuration._validate_config_file("foo")
        mock_exists.assert_called_once_with("foo")

    @mock.patch.object(os.path, 'exists', autospec=True)
    @mock.patch.object(os.path, 'isfile', autospec=True)
    def test__validate_config_file_not_a_file(
            self, mock_isfile, mock_exists):
        mock_exists.return_value = True
        mock_isfile.return_value = False
        with pytest.raises(exception.ConfigurationError):
            configuration._validate_config_file("foo")
        mock_exists.assert_called_once_with("foo")
        mock_isfile.assert_called_once_with("foo")

    def test_update_configuration(self):
        default_config = configuration.DEFAULTS
        update_dict = {
            "pypi_index_url": "http://pypi-ninja.ninjacorp.com/simple"}
        updated_config = configuration.update_configuration(
            default_config, update_dict)
        assert default_config.pypi_index_url == None
        assert updated_config.pypi_index_url == \
                       "http://pypi-ninja.ninjacorp.com/simple"
