"""Sub-command base class that all sub-commands should inherit in order to
implement the add_parser and run functions.
"""
from __future__ import absolute_import

import collections
import sys

from pyleus.configuration import load_configuration
from pyleus.configuration import update_configuration
from pyleus.exception import command_error_fmt
from pyleus.exception import PyleusError
from pyleus.utils import expand_path


CMD = "pyleus"


SubCommandInfo = collections.namedtuple(
    "SubCommandInterface",
    "command_name usage description help_msg"
)


class SubCommand(object):
    """Sub-command base class"""

    @staticmethod
    def get_sub_command_info():
        """Return a SubCommandInfo named tuple containing
        the info that should be visualized on the command-line
        for the sub-command.

        Args:
            command_name: mandatory
            usage: argparse default if None
            description: argparse default if None
            help_msg: argparse default if None
        """
        raise NotImplementedError

    @staticmethod
    def add_arguments(parser):
        """Define arguments and options of the sub-command
        in an argparse-fashion way.
        """
        raise NotImplementedError

    @staticmethod
    def run(configs):
        """Callback associated to the sub-command.
        Implement the sub-command logic here
        """
        raise NotImplementedError

    @classmethod
    def add_parser(cls, subparsers):
        """During the top-level command definition, it needs to be called
        in order to register the sub-command.

        Args:
            subparsers: the result value of the
                top_level_parser.add_subparsers() method
        """
        info = cls.get_sub_command_info()
        parser = subparsers.add_parser(
            info.command_name,
            usage=info.usage,
            description=info.description,
            help=info.help_msg,
            add_help=False)

        # the help message need to be regenerated, since help has been
        # suppressed in the parser definition
        parser.add_argument(
            "-h", "--help", action="help",
            help="Show this message and exit")
        cls.add_arguments(parser)

        # Set the callback associated to the sub-command
        parser.set_defaults(func=cls.run_subcommand)

    @classmethod
    def run_subcommand(cls, arguments):
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
            sys.exit(command_error_fmt(CMD, e))

        # Update configuration with command line values
        configs = update_configuration(configs, vars(arguments))

        cls.run(configs)
