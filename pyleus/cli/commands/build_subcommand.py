"""Sub-command for building a standalone, self-contained Pyleus topology
JAR ready to be submitted to a Storm cluster. If an optional requirements file
is provided, Pyleus will use virtualenv to collect and provide Python
dependencies to the running topology.

Args:
    TOPOLOGY_PATH - the path to a topology YAML file, defaulting to
        'pyleus_topology.yaml' in the current directory.

The output JAR is built from a common base JAR included in the pyleus package
by default, and will be named <TOPOLOGY_NAME>.jar.
"""
from __future__ import absolute_import

from pyleus.cli.commands.subcommand import SubCommand
from pyleus.cli.build import build_topology_jar
from pyleus.configuration import DEFAULTS


class BuildSubCommand(SubCommand):
    """Build subcommand class."""

    NAME = "build"
    DESCRIPTION = "Build a Storm jar from a Pyleus topology file"

    def add_arguments(self, parser):
        parser.add_argument(
            "topology_path", metavar="TOPOLOGY_PATH", nargs="?",
            default=DEFAULTS.topology_path,
            help="Path to Pyleus topology file. Default: %(default)s")
        parser.add_argument(
            "-o", "--output", dest="output_jar", help="Path of the jar to be "
            "written. Default: <topology_name>.jar")
        parser.add_argument(
            "-s", "--system-site-packages", dest="system_site_packages",
            action="store_true",
            help="Do not install packages already present on your system.")

    def run(self, configs):
        build_topology_jar(configs)
