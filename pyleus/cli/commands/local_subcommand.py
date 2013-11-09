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

from pyleus.cli.run import run_topology_locally
from pyleus.cli.commands.run_subcommand import RunSubCommand
from pyleus.cli.commands.subcommand import SubCommandInfo


CMD = "local"


class LocalSubCommand(RunSubCommand):
    """Run subcommand class"""

    def get_sub_command_info(self):
        return SubCommandInfo(
            command_name=CMD,
            usage="%(prog)s [options] TOPOLOGY_PATH",
            description="Run a Pyleus topology locally."
                        " WARNING: DO NOT USE THIS COMMAND YET."
                        " YOUR LOCAL MACHINE WILL ATROCIOUSLY DIE.",
            help_msg="Run a Pyleus topology locally."
                     " WARNING: DO NOT USE THIS COMMAND YET."
                     " YOUR LOCAL MACHINE WILL ATROCIOUSLY DIE.")

    def run_topology(self, jar_path, configs):
        run_topology_locally(jar_path, configs)
