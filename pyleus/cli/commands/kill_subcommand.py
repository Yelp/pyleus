"""Sub-command for killing topologies running on a Storm cluster.
"""
from __future__ import absolute_import

from pyleus.cli.commands.subcommand import SubCommand
from pyleus.cli.topologies import add_storm_cluster_ip_argument
from pyleus.cli.topologies import kill_topology


class KillSubCommand(SubCommand):
    """Kill subcommand class."""

    NAME = "kill"
    DESCRIPTION = "Kill a topology running on a Storm cluster"

    def add_arguments(self, parser):
        parser.add_argument(
            "topology_name", metavar="TOPOLOGY_NAME",
            help="Name of the topology to kill.")
        parser.add_argument(
            "-w", "--wait-time", dest="wait_time",
            help="Override the duration in seconds Storm waits between "
                 "deactivation and shutdown.")
        add_storm_cluster_ip_argument(parser)

    def run(self, configs):
        kill_topology(configs)
