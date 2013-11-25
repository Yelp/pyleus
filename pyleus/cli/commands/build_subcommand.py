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

import sys

from pyleus.cli.commands.subcommand import SubCommand
from pyleus.cli.commands.subcommand import SubCommandInfo
from pyleus.cli.build import build_topology_jar
from pyleus.exception import command_error_fmt
from pyleus.exception import PyleusError


CMD = "build"


class BuildSubCommand(SubCommand):
    """Build subcommand class"""

    def get_sub_command_info(self):
        return SubCommandInfo(
            command_name=CMD,
            usage="%(prog)s [options] TOPOLOGY_DIRECTORY",
            description="Build up a Storm jar from a topology source"
                        " directory",
            help_msg="Build up a Storm jar from a topology source directory")

    def add_arguments(self, parser):
        parser.add_argument(
            "topology_dir", metavar="TOPOLOGY_DIRECTORY",
            help="directory containing topology source code")
        parser.add_argument(
            "-o", "--out", dest="output_jar",
            help="Path of the jar file that will contain"
            " all the dependencies and the resources")
        parser.add_argument(
            "-s", "--system-site-packages",
            dest="system_site_packages", action="store_true",
            help="Do not install packages already present"
            "on your system")

    def run(self, configs):
        try:
            build_topology_jar(configs)
        except PyleusError as e:
            sys.exit(command_error_fmt(CMD, e))
