from __future__ import absolute_import

import os
import sys
import zipfile

from pyleus.cli.build import build_topology_jar
from pyleus.exception import command_error_fmt
from pyleus.exception import PyleusError
from pyleus.exception import TopologyError
from pyleus.utils import expand_path


def _run_topology_jar():
    pass


def run_topology(mode, configs):
    """Parse command-line arguments, build a Pyleus jar from the topology
    directory, if it is the case,  and invoke _run_topology_jar()
    """

    topo_path = expand_path(configs.topology_dir)
    jar_path = topo_path

    # if the path points to a directory
    if os.path.isdir(topo_path):
        try:
            jar_path = build_topology_jar(configs)
        except PyleusError as e:
            sys.exit(command_error_fmt(mode, e))

    # if the file is actually a jar
    if zipfile.is_zipfile(jar_path):
        try:
            _run_topology_jar(mode, jar_path, configs.storm_cluster_ip)
        except PyleusError as e:
            sys.exit(command_error_fmt(mode, e))
    else:
        e = TopologyError("The topology specified is not jar")
        sys.exit(command_error_fmt(mode, e))
