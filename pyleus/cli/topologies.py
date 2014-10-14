"""Logic for topologies management.
"""
from __future__ import absolute_import

import zipfile

from pyleus.cli.storm_cluster import LocalStormCluster
from pyleus.cli.storm_cluster import StormCluster


def add_storm_cluster_ip_argument(parser):
    """Add to the command parser an option in order to specify the cluster
    ip address from command line.
    """
    parser.add_argument(
        "-n", "--nimbus-ip", dest="storm_cluster_ip", metavar="NIMBUS_IP",
        help="The Nimbus IP address of the Storm cluster to query")


def run_topology_locally(jar_path, configs):
    """Run the pyleus topology jar on the local machine."""
    LocalStormCluster().run(
        configs.storm_cmd_path,
        jar_path,
        configs.debug,
        configs.jvm_opts)


def submit_topology(jar_path, configs):
    """Submit the topology jar to the Storm cluster specified in configs."""
    StormCluster(
        configs.storm_cmd_path,
        configs.storm_cluster_ip,
        configs.verbose,
        configs.jvm_opts).submit(jar_path)


def list_topologies(configs):
    """List the topologies running on the Storm cluster specified in configs."""
    StormCluster(
        configs.storm_cmd_path,
        configs.storm_cluster_ip,
        configs.verbose,
        configs.jvm_opts).list()


def kill_topology(configs):
    """Kill a topology running on the Storm cluster specified in configs."""
    StormCluster(
        configs.storm_cmd_path,
        configs.storm_cluster_ip,
        configs.verbose,
        configs.jvm_opts).kill(configs.topology_name, configs.wait_time)


def is_jar(jar_path):
    return zipfile.is_zipfile(jar_path)
