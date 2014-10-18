"""Sub-command for running a Pyleus topology on a Storm cluster.

Args:
    TOPOLOGY_JAR - The path to the Pyleus jar to run.
"""
from __future__ import absolute_import

from pyleus.cli.topologies import add_nimbus_arguments
from pyleus.cli.topologies import submit_topology
from pyleus.cli.commands.run_subcommand import RunSubCommand


class SubmitSubCommand(RunSubCommand):
    """Submit subcommand class."""

    NAME = "submit"
    DESCRIPTION = "Submit a Pyleus topology to a Storm cluster"

    def add_specific_arguments(self, parser):
        add_nimbus_arguments(parser)

    def run_topology(self, jar_path, configs):
        submit_topology(jar_path, configs)
