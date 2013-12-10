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
from pyleus.cli.commands.subcommand import SubCommandInfo


CMD = "local"


class LocalSubCommand(RunSubCommand):
    """Run subcommand class"""

    def get_sub_command_info(self):
        return SubCommandInfo(
            command_name=CMD,
            usage="%(prog)s [options] TOPOLOGY_PATH",
            description="Run a Pyleus topology locally.",
            help_msg="Run a Pyleus topology locally.")

    def run_topology(self, jar_path, configs):
        run_topology_locally(jar_path, configs)
