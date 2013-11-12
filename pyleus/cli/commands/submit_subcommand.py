"""Sub-command for running a Pyleus topology on a Storm cluster.

Args:
    TOPOLOGY_PATH - If the path to a directory containing the topology source
        code is specified, a Pyleus jar will be created on the fly before
        execution. If the path to a Pyleus jar is specified, the jar will be
        processed for execution immediately.
"""
from __future__ import absolute_import

from pyleus.cli.topologies import add_storm_cluster_ip_argument
from pyleus.cli.topologies import submit_topology
from pyleus.cli.commands.run_subcommand import RunSubCommand
from pyleus.cli.commands.subcommand import SubCommandInfo


CMD = "submit"


class SubmitSubCommand(RunSubCommand):
    """Run subcommand class"""

    def get_sub_command_info(self):
        return SubCommandInfo(
            command_name=CMD,
            usage="%(prog)s [options] TOPOLOGY_PATH",
            description="Submit a Pyleus topology to a Storm cluster for"
                        " execution",
            help_msg="Submit a Pyleus topology to a Storm cluster for"
                     " execution")

    def add_specific_arguments(self, parser):
        add_storm_cluster_ip_argument(parser)

    def run_topology(self, jar_path, configs):
        submit_topology(jar_path, configs)
