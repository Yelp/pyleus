"""Sub-command for building a standalone, self-contained Pyleus topology
JAR ready to be submitted to a Storm cluster. If an optional requirements.txt
is provided, Pyleus will use virtualenv to collect and provide Python
dependencies to the running topology.

Args:
    TOPOLOGY_DIRECTORY - the directory where all the topology source files,
        the YAML file describing the topology (pyleus_topology.yaml) and the
        optional requirements.txt are found.

The script will attempt to ensure the contents of TOPOLOGY_DIRECTORY are in
order, that nothing will be improperly overwritten and that mandatory files are
present: pyleus_topology.yaml is always required and requirements.txt must
exist if --use-virtualenv is explicitly stated.

The output JAR is built from a common base JAR included in the pyleus package
by default, and will be named <TOPOLOGY_DIRECTORY>.jar.

Note: The names used for the YAML file and for the virtualenv CANNOT be changed
without modifying the Java code accordingly.
"""
from __future__ import absolute_import

from pyleus.cli.commands.subcommand import SubCommand
from pyleus.cli.build import build_topology_jar
from pyleus.configuration import DEFAULTS


class BuildSubCommand(SubCommand):

    NAME = "build"
    DESCRIPTION = "Build a Storm jar from a Pyleus topology file"

    def add_arguments(self, parser):
        parser.add_argument(
            "topology_path", metavar="TOPOLOGY_PATH", nargs="?",
            default=DEFAULTS.topology_path, help="Path to Pyleus topology file "
            "Default: %(default)s")
        parser.add_argument(
            "-o", "--output", dest="output_jar", help="Path of the jar to be "
            "written. Default: <topology_name>.jar")
        parser.add_argument(
            "-s", "--system-site-packages", dest="system_site_packages",
            action="store_true", help="Do not install packages already present "
            "on your system.")

    def run(self, configs):
        build_topology_jar(configs)
