"""Sub-command for running a Pyleus topology in local mode.

Args:
    TOPOLOGY_PATH - If the path to a directory containing the topology source
        code is specified, a Pyleus jar will be created on the fly before
        execution. If the path to a Pyleus jar is specified, the jar will be
        processed for execution immediately.

Note: In order to trigger the local mode for the selected topology,
PyleusTopologyBuilder needs to be called with the option <--local>.
"""
from __future__ import absolute_import

from pyleus.cli.topologies import run_topology_locally
from pyleus.cli.commands.run_subcommand import RunSubCommand


class LocalSubCommand(RunSubCommand):
    """Run subcommand class"""

    NAME = "local"
    USAGE = "%(prog)s [options] TOPOLOGY_PATH"
    DESCRIPTION = "Run a Pyleus topology locally."
    HELP_MSG = "Run a Pyleus topology locally."

    def run_topology(self, jar_path, configs):
        run_topology_locally(jar_path, configs)
