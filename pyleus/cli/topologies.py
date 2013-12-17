"""Logic for topologies management.
"""
from __future__ import absolute_import

import os
import zipfile

from pyleus.cli.build import build_topology_jar
from pyleus.cli.storm_cluster import LocalStormCluster
from pyleus.cli.storm_cluster import StormCluster
from pyleus.exception import TopologyError
from pyleus.utils import expand_path


def add_storm_cluster_ip_argument(parser):
    """Add to the command parser an option in order to specify the cluster
    ip address from command line.
    """
    parser.add_argument(
        "-s", "--storm-cluster", dest="storm_cluster_ip",
        help="The Nimbus IP address of the Storm cluster to query")


def run_topology_locally(jar_path, configs):
    """Run the pyleus topology jar on the local machine"""
    LocalStormCluster().run(jar_path, configs.debug)


def submit_topology(jar_path, configs):
    """Submit the topology jar to the Storm cluster specified in configs"""
    StormCluster(configs.storm_cluster_ip, configs.verbose).submit(jar_path)


def list_topologies(configs):
    """List the topologies running on the Storm cluster specified in configs"""
    StormCluster(configs.storm_cluster_ip, configs.verbose).list()


def kill_topology(configs):
    """Kill a topology running on the Storm cluster specified in configs"""
    StormCluster(configs.storm_cluster_ip, configs.verbose).kill(
        configs.topology_name, configs.wait_time)


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
