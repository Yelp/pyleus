import __builtin__

import mock
import pytest

from pyleus.cli.commands import subcommand
from pyleus.cli.commands.subcommand import SubCommand


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
    @mock.patch.object(subcommand, "update_configuration", autospec=True)
    @mock.patch.object(__builtin__, "vars", autospec=True)
    @mock.patch.object(SubCommand, "run")
    def test_run_subcommand(self, mock_run, mock_vars,
                            mock_update, mock_load, mock_expand):
        mock_args = mock.Mock(config_file="foo")
        mock_expand.return_value = "bar"
        self.subcmd.run_subcommand(mock_args)
        mock_expand.assert_called_once_with("foo")
        mock_load.assert_called_once_with("bar")
        mock_update.assert_called_once_with(mock_load.return_value,
                                            mock_vars(mock_args))
        mock_run.assert_called_once_with(mock_update.return_value)
