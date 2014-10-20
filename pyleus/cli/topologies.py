"""Logic for topologies management.
"""
from __future__ import absolute_import

import zipfile

from pyleus.cli.storm_cluster import LocalStormCluster
from pyleus.cli.storm_cluster import StormCluster


def add_nimbus_arguments(parser):
    """Add Nimbus host/port arguments to the command parser."""
    parser.add_argument(
        "-n", "--nimbus-host", dest="nimbus_host", metavar="NIMBUS_HOST",
        help="The hostname or IP address of the Storm cluster's Nimbus node")
    parser.add_argument(
        "-p", "--nimbus-port", dest="nimbus_port", metavar="NIMBUS_PORT",
        help="The Thrift port used by the Storm cluster's Nimbus node")


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
        configs.nimbus_host,
        configs.nimbus_port,
        configs.verbose,
        configs.jvm_opts).submit(jar_path)


def list_topologies(configs):
    """List the topologies running on the Storm cluster specified in configs."""
    StormCluster(
        configs.storm_cmd_path,
        configs.nimbus_host,
        configs.nimbus_port,
        configs.verbose,
        configs.jvm_opts).list()


def kill_topology(configs):
    """Kill a topology running on the Storm cluster specified in configs."""
    StormCluster(
        configs.storm_cmd_path,
        configs.nimbus_host,
        configs.nimbus_port,
        configs.verbose,
        configs.jvm_opts).kill(configs.topology_name, configs.wait_time)


def is_jar(jar_path):
    return zipfile.is_zipfile(jar_path)
