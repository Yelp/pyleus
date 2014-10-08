"""Sub-command base class for commands able to run a topology, as local or
submit, starting either from a jar or from a pyleus topology source directory.

Args:
    TOPOLOGY_JAR - The path to the Pyleus jar to run.
"""
from __future__ import absolute_import

from pyleus.cli.commands.subcommand import SubCommand
from pyleus.cli.topologies import is_jar


class RunSubCommand(SubCommand):
    """Run subcommand class."""

    # Override these in subclass
    NAME = None
    DESCRIPTION = None

    def add_specific_arguments(self, parser):
        """Override this method in order to add subcommand specific
        arguments.
        """
        pass

    def add_arguments(self, parser):
        parser.add_argument(
            "topology_jar", metavar="TOPOLOGY_JAR",
            help="Path to a Pyleus topology jar.")
        self.add_specific_arguments(parser)

    def run_topology(jar_path, configs):
        """Subcommand specific run logic."""
        raise NotImplementedError

    def run(self, configs):
        jar_path = configs.topology_jar

        if not is_jar(jar_path):
            self.error("Invalid jar: {0}".format(jar_path))

        self.run_topology(jar_path, configs)
