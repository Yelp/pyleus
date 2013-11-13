"""Sub-command for listing all topologies running on a Storm cluster.
"""
from __future__ import absolute_import

import sys

from pyleus.cli.commands.subcommand import SubCommand
from pyleus.cli.commands.subcommand import SubCommandInfo
from pyleus.cli.topologies import add_storm_cluster_ip_argument
from pyleus.cli.topologies import list_topologies
from pyleus.exception import command_error_fmt
from pyleus.exception import PyleusError


CMD = "list"


class ListSubCommand(SubCommand):
    """List subcommand class"""

    def get_sub_command_info(self):
        return SubCommandInfo(
            command_name=CMD,
            usage="%(prog)s [--storm-cluster STORM_CLUSTER_IP]",
            description="List all the topology running on a Storm cluster.",
            help_msg="List all the topology running on a Storm cluster.")

    def add_arguments(self, parser):
        add_storm_cluster_ip_argument(parser)

    def run(self, configs):
        try:
            list_topologies(configs)
        except PyleusError as e:
            sys.exit(command_error_fmt(CMD, e))
