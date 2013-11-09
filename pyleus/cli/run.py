"""Logic for running a pyleus topology either locally or on a Storm cluster.

The caller function should handle PyleusError exceptions.
"""
from __future__ import absolute_import

import os
import zipfile

from pyleus.cli.build import build_topology_jar
from pyleus.exception import TopologyError
from pyleus.utils import expand_path


def run_topology_locally(jar_path, configs):
    pass


def submit_topology(jar_path, configs):
    pass


def get_runnable_jar_path(configs):
    """Parse command-line arguments, build a Pyleus jar from the topology
    directory if it is the case, and return the path to the runnable jar.
    """
    topo_path = expand_path(configs.topology_dir)

    if not os.path.exists(topo_path):
        raise TopologyError(
            "Topology not found: {0}".format(topo_path))

    # if the path points to a directory, build a jar out of it
    if os.path.isdir(topo_path):
        return build_topology_jar(configs)

    # if the file is actually a jar, just return its expanded path
    elif zipfile.is_zipfile(topo_path):
        return topo_path

    raise TopologyError(
        "The topology specified is nor a directory nor jar:{0}"
        .format(topo_path))
