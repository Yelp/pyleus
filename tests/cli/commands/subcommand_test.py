import pytest

from pyleus.cli.commands import subcommand
from pyleus.cli.commands.subcommand import SubCommand
from pyleus.exception import ConfigurationError
from pyleus.testing import mock, builtins


class TestSubCommand(object):

    @pytest.fixture(autouse=True)
    def setup(self):
        self.subcmd = SubCommand()

    def test_add_arguments(self):
        mock_parser = mock.Mock()
        with pytest.raises(NotImplementedError):
            self.subcmd.add_arguments(mock_parser)

    def test_run(self):
        mock_configs = mock.Mock()
        with pytest.raises(NotImplementedError):
            self.subcmd.run(mock_configs)

    @mock.patch.object(subcommand, "expand_path", autospec=True)
    @mock.patch.object(subcommand, "load_configuration", autospec=True)
    @mock.patch.object(
        subcommand, "_ensure_storm_path_in_configs", autospec=True)
    @mock.patch.object(subcommand, "update_configuration", autospec=True)
    @mock.patch.object(builtins, "vars", autospec=True)
    @mock.patch.object(SubCommand, "run")
    def test_run_subcommand(
            self, mock_run, mock_vars, mock_update, mock_ensure, mock_load,
            mock_expand):
        mock_args = mock.Mock(config_file="foo")
        mock_expand.return_value = "bar"

        self.subcmd.run_subcommand(mock_args)

        mock_expand.assert_called_once_with("foo")
        mock_load.assert_called_once_with("bar")
        mock_ensure.assert_called_once_with(mock_load.return_value)
        mock_update.assert_called_once_with(
            mock_ensure.return_value, mock_vars(mock_args))
        mock_run.assert_called_once_with(mock_update.return_value)

    @mock.patch.object(subcommand, "search_storm_cmd_path", autospec=True)
    @mock.patch.object(subcommand, "update_configuration", autospec=True)
    def test__ensure_storm_path_in_configs_path_defined(
            self, mock_update, mock_search):
        mock_configs = mock.Mock(specs=['storm_cmd_path'])

        configs = subcommand._ensure_storm_path_in_configs(mock_configs)

        assert mock_search.called == 0
        assert mock_update.called == 0
        assert configs == mock_configs

    @mock.patch.object(subcommand, "search_storm_cmd_path", autospec=True)
    @mock.patch.object(subcommand, "update_configuration", autospec=True)
    def test__ensure_storm_path_in_configs_path_not_defined(
            self, mock_update, mock_search):
        mock_configs = mock.Mock(specs=['storm_cmd_path'])
        mock_configs.storm_cmd_path = None

        configs = subcommand._ensure_storm_path_in_configs(mock_configs)

        mock_search.assert_called_with()
        mock_update.assert_called_with(
            mock_configs, {'storm_cmd_path': mock_search.return_value})
        assert configs == mock_update.return_value

    @mock.patch.object(subcommand, "search_storm_cmd_path", autospec=True)
    def test__ensure_storm_path_in_configs_path_not_found(self, mock_search):
        mock_configs = mock.Mock(specs=['storm_cmd_path'])
        mock_configs.storm_cmd_path = None
        mock_search.return_value = None

        with pytest.raises(ConfigurationError):
            subcommand._ensure_storm_path_in_configs(mock_configs)

            mock_search.assert_called_with()
