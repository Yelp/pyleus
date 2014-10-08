"""Sub-command base class that all sub-commands should inherit in order to
implement the add_parser and run functions.
"""
from __future__ import absolute_import

import argparse
import sys

from pyleus.configuration import load_configuration
from pyleus.configuration import update_configuration
from pyleus.exception import command_error_fmt
from pyleus.exception import ConfigurationError
from pyleus.exception import PyleusError
from pyleus.utils import expand_path
from pyleus.utils import search_storm_cmd_path


def _ensure_storm_path_in_configs(configs):
    """Ensure that the storm executable path is in the configuration.
    If not present, search for it and update the config.

    :raise: ConfigurationError if unable to locate storm path
    """

    if configs.storm_cmd_path is not None:
        return configs

    storm_cmd_path = search_storm_cmd_path()
    if storm_cmd_path is None:
        raise ConfigurationError(
            "Unable to locate Storm executable. Please either install "
            "Storm or specify its path in your configuration file")

    return update_configuration(configs, {"storm_cmd_path": storm_cmd_path})


class SubCommand(object):
    """Base class for pyleus subcommands."""

    # Override these in subclass
    NAME = None
    DESCRIPTION = None

    def add_arguments(self, parser):
        """Define arguments and options of the sub-command
        in an argparse-fashion way.

        Please also add options in the Configuration named tuple
        in configuration.py. Specify default values ONLY in the
        default Configuration tuple in configuration.py.
        """
        raise NotImplementedError

    def run(self, configs):
        """Callback associated to the sub-command.
        Implement the sub-command logic here.
        """
        raise NotImplementedError

    def init_subparser(self, subparsers):
        """During the top-level command definition, it needs to be called
        in order to register the sub-command.

        :param subparsers:
         the result value of the top_level_parser.add_subparsers() method
        """
        parser = subparsers.add_parser(
            self.NAME,
            argument_default=argparse.SUPPRESS,
            description=self.DESCRIPTION,
            help=self.DESCRIPTION,
            add_help=False)

        # the help message need to be regenerated, since help has been
        # suppressed in the parser definition
        parser.add_argument(
            "-h", "--help", action="help",
            help="Show this message and exit")
        self.add_arguments(parser)

        # Set the callback associated to the sub-command
        parser.set_defaults(func=self.run_subcommand)

    def run_subcommand(self, arguments):
        """Load the configuration, update it with the arguments and options
        specified on the command-line and then call the run method implemented
        by each sub-command.
        """
        # Expand path of the command-line specified config file, if any
        if arguments.config_file is not None:
            arguments.config_file = expand_path(arguments.config_file)

        # Load configurations into a Configuration named tuple
        try:
            configs = load_configuration(arguments.config_file)
        except PyleusError as e:
            self.error(e)

        configs = _ensure_storm_path_in_configs(configs)

        # Update configuration with command line values
        configs = update_configuration(configs, vars(arguments))

        try:
            self.run(configs)
        except PyleusError as e:
            self.error(e)

    def error(self, message):
        """Exit and print error message."""
        sys.exit(command_error_fmt(self.NAME, message))
