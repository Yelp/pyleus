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

from pyleus.cli.run import run_topology
from pyleus.cli.commands.subcommand import SubCommand
from pyleus.cli.commands.subcommand import SubCommandInfo
from pyleus.exception import command_error_fmt
from pyleus.exception import PyleusError


CMD_LOCAL = "local"
CMD_SUBMIT = "submit"


class RunSubCommand(SubCommand):
    """Run subcommand class"""

    def get_sub_command_info(self):
        # If no action keyword is specified
        if self.action is None:
            e = PyleusError("Unspecified RunSubCommand action keyword")
            sys.exit(command_error_fmt(self.__class__.__name__, e))

        if (self.action == CMD_SUBMIT):
            return SubCommandInfo(
                command_name=CMD_SUBMIT,
                usage="%(prog)s [options] TOPOLOGY_PATH",
                description="Submit a Pyleus topology to a Storm cluster for"
                            " execution",
                help_msg="Submit a Pyleus topology to a Storm cluster for"
                         " execution")
        if (self.action == CMD_LOCAL):
            return SubCommandInfo(
                command_name=CMD_LOCAL,
                usage="%(prog)s [options] TOPOLOGY_PATH",
                description="Run a Pyleus topology locally."
                            " WARNING: DO NOT USE THIS COMMAND YET."
                            " YOUR LOCAL MACHINE WILL ATROCIOUSLY DIE.",
                help_msg="Run a Pyleus topology locally."
                         " WARNING: DO NOT USE THIS COMMAND YET."
                         " YOUR LOCAL MACHINE WILL ATROCIOUSLY DIE.")

        # If another action keyword is specified
        e = PyleusError("Invalid RunSubCommand action keyword")
        sys.exit(command_error_fmt(self.__class__.__name__, e))

    def add_arguments(self, parser):
        parser.add_argument(
            "topology_dir", metavar="TOPOLOGY_PATH",
            help="If the path to a directory containing the topology source"
                 " code is specified, a Pyleus jar will be created on the fly"
                 " before execution."
                 " If the path to a Pyleus jar is specified, the jar will be"
                 "processed for execution immediately.")
        if(self.action == CMD_SUBMIT):
            parser.add_argument(
                "-s", "--storm-cluster", dest="storm_cluster_ip",
                help="The Nimbus IP address of the Strom cluster the job will"
                     " be submitted to.")

    def run(self, configs):
        try:
            run_topology(self.action, configs)
        except PyleusError as e:
            sys.exit(command_error_fmt(self.action, e))
