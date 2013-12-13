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


class SubmitSubCommand(RunSubCommand):
    """Run subcommand class"""

    NAME = "submit"
    USAGE = "%(prog)s [options] TOPOLOGY_PATH"
    DESCRIPTION = "Submit a Pyleus topology to a Storm cluster for execution"
    HELP_MSG = "Submit a Pyleus topology to a Storm cluster for execution"

    def add_specific_arguments(self, parser):
        add_storm_cluster_ip_argument(parser)

    def run_topology(self, jar_path, configs):
        submit_topology(jar_path, configs)
