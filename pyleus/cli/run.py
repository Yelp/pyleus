"""Logic for running a pyleus topology either locally or on a Storm cluster.

The caller function should handle PyleusError exceptions.
"""
from __future__ import absolute_import

import os
import socket
import subprocess
import zipfile

from pyleus.cli.build import build_topology_jar
from pyleus.exception import ConfigurationError
from pyleus.exception import StormError
from pyleus.exception import TopologyError
from pyleus.utils import expand_path


STORM_PATH = "/usr/share/storm/bin/storm"
TOPOLOGY_BUILDER_CLASS = "com.yelp.pyleus.PyleusTopologyBuilder"


def _validate_ip_address(address):
    try:
        socket.inet_aton(address)
    except socket.error:
        raise ConfigurationError("The IP address specified is invalid: {0}"
                                 .format(address))


def _exec_storm_cmd(cmd, verbose, err_msg):
    """Interface to any storm command"""
    out_stream = None
    if not verbose:
        out_stream = open(os.devnull, "w")
    storm_cmd = [STORM_PATH]
    storm_cmd += cmd
    proc = subprocess.Popen(storm_cmd,
                            stdout=out_stream,
                            stderr=subprocess.STDOUT)
    out_data, _ = proc.communicate()
    if proc.returncode != 0:
        raise StormError(err_msg)
    return out_data


def run_topology_locally(jar_path, configs):
    pass


def submit_topology(jar_path, configs):
    """Submit the topology jar to the Storm cluster specified in configs"""

    cmd = ["jar"]
    if configs.storm_cluster_ip is None:
        raise ConfigurationError(
            "You must specify a storm cluster IP address."
            " Use option <storm_cluster_ip> in the configuration file"
            " or the command line option --storm-cluster")

    _validate_ip_address(configs.storm_cluster_ip)

    cmd += [
        "-c",
        "nimbus.host={0}".format(configs.storm_cluster_ip),
        jar_path,
        TOPOLOGY_BUILDER_CLASS
    ]

    _exec_storm_cmd(cmd, configs.verbose,
                    "Storm command failed. Run with --verbose for more info.")


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
