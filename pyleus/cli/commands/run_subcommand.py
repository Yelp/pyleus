"""Subcommand base class for commands able to run a topology, as local or
submit, starting either from a jar or from a pyleus topology source directory.

Args:
    TOPOLOGY_PATH - If the path to a directory containing the topology source
        code is specified, a Pyleus jar will be created on the fly before
        execution. If the path to a Pyleus jar is specified, the jar will be
        processed for execution immediately.
"""
from __future__ import absolute_import

import sys

from pyleus.cli.topologies import get_runnable_jar_path
from pyleus.cli.commands.subcommand import SubCommand
from pyleus.exception import command_error_fmt
from pyleus.exception import PyleusError


class RunSubCommand(SubCommand):
    """Run subcommand class"""

    # Override these in subclass
    NAME = None
    USAGE = "%(prog)s [options] TOPOLOGY_PATH"
    DESCRIPTION = None
    HELP = None

    def add_specific_arguments(self, parser):
        """Override this method in order to add subcommand specific
        arguments.
        """
        pass

    def add_arguments(self, parser):
        parser.add_argument(
            "topology_dir", metavar="TOPOLOGY_PATH",
            help="If the path to a directory containing the topology source "
                 "code is specified, a Pyleus jar will be created on the fly "
                 "before execution. If the path to a Pyleus jar is specified, "
                 "the jar will be processed for execution immediately.")
        self.add_specific_arguments(parser)

    def run_topology(jar_path, configs):
        """Subcommand specific run logic."""
        raise NotImplementedError

    def run(self, configs):
        try:
            jar_path = get_runnable_jar_path(configs)
            self.run_topology(jar_path, configs)
        except PyleusError as e:
            sys.exit(
                command_error_fmt(self.NAME, e))
