"""Sub-command for running a Pyleus topology in local mode.

Args:
    TOPOLOGY_JAR - The path to the Pyleus jar to run.
"""
from __future__ import absolute_import

from pyleus.cli.topologies import run_topology_locally
from pyleus.cli.commands.run_subcommand import RunSubCommand


class LocalSubCommand(RunSubCommand):
    """Local subcommand class."""

    NAME = "local"
    DESCRIPTION = "Run a Pyleus topology locally"

    def add_specific_arguments(self, parser):
        parser.add_argument(
            "-d", "--debug", dest="debug", action="store_true", help="Enable "
            "Storm debug logging for all components in the topology.")

    def run_topology(self, jar_path, configs):
        run_topology_locally(jar_path, configs)
