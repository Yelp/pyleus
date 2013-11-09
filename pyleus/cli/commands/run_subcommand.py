"""Sub-command for running a Pyleus topology both either in local mode
or on a Storm cluster. In order to descriminate between the two modes,
the <action> attribute is used.

Args:
    TOPOLOGY_PATH - If the path to a directory containing the topology source
        code is specified, a Pyleus jar will be created on the fly before
        execution. If the path to a Pyleus jar is specified, the jar will be
        processed for execution immediately.

Note: In order to trigger the local mode for the selcted topology,
PyleusTopologyBuilder needs to be called with the option <--local>.
"""
from __future__ import absolute_import

import sys

from pyleus.cli.run import get_runnable_jar_path
from pyleus.cli.commands.subcommand import SubCommand
from pyleus.exception import command_error_fmt
from pyleus.exception import PyleusError


class RunSubCommand(SubCommand):
    """Run subcommand class"""

    def add_specific_arguments(self, parser):
        """Override this method in order to add subcommand specific
        arguments.
        """
        pass

    def add_arguments(self, parser):
        parser.add_argument(
            "topology_dir", metavar="TOPOLOGY_PATH",
            help="If the path to a directory containing the topology source"
                 " code is specified, a Pyleus jar will be created on the fly"
                 " before execution."
                 " If the path to a Pyleus jar is specified, the jar will be"
                 "processed for execution immediately.")
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
                command_error_fmt(self.get_sub_command_info().command_name, e))
