"""Sub-command for killing topologies running on a Storm cluster.
"""
from __future__ import absolute_import

import sys

from pyleus.cli.commands.subcommand import SubCommand
from pyleus.cli.commands.subcommand import SubCommandInfo
from pyleus.cli.topologies import add_storm_cluster_ip_argument
from pyleus.cli.topologies import kill_topology
from pyleus.exception import command_error_fmt
from pyleus.exception import PyleusError


CMD = "kill"


class KillSubCommand(SubCommand):
    """Kill subcommand class"""

    def get_sub_command_info(self):
        return SubCommandInfo(
            command_name=CMD,
            usage="%(prog)s [options] TOPOLOGY_NAME",
            description="Kill a topology running on a Storm cluster.",
            help_msg="Kill a topology running on a Storm cluster.")

    def add_arguments(self, parser):
        parser.add_argument(
            "topology_name", metavar="TOPOLOGY_NAME",
            help="Name of the topology to delete.")
        parser.add_argument(
            "-w", "--wait-time", dest="wait_time",
            help="Override length of time Storm waits between deactivation and"
                 " shutdown.")
        add_storm_cluster_ip_argument(parser)

    def run(self, configs):
        try:
            kill_topology(configs)
        except PyleusError as e:
            sys.exit(command_error_fmt(CMD, e))
