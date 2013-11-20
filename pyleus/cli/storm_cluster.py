"""Interface classes to Storm clusters. As things are, they implement
cluster-topologies interactions, but not cluster management.
"""
from __future__ import absolute_import

import os
import socket
import subprocess

from pyleus.exception import ConfigurationError
from pyleus.exception import StormError


STORM_PATH = "/usr/share/storm/bin/storm"
TOPOLOGY_BUILDER_CLASS = "com.yelp.pyleus.PyleusTopologyBuilder"
LOCAL_OPTION = "--local"


def _validate_ip_address(address):
    """Ensure that the given address is valid.

    Note: IPv4 only.
    """
    try:
        socket.inet_aton(address)
    except socket.error:
        raise ConfigurationError("The IP address specified is invalid: {0}"
                                 .format(address))


def _exec_storm_cmd(cmd, nimbus_ip, verbose):
    """Interface to any storm command"""
    out_stream = None
    if not verbose:
        out_stream = open(os.devnull, "w")

    storm_cmd = [STORM_PATH]
    storm_cmd += cmd
    if nimbus_ip is not None:
        storm_cmd += ["-c", "nimbus.host={0}".format(nimbus_ip)]

    proc = subprocess.Popen(storm_cmd,
                            stdout=out_stream,
                            stderr=subprocess.STDOUT)
    out_data, _ = proc.communicate()
    if proc.returncode != 0:
        raise StormError(
            "Storm command failed. Run with --verbose for more info.")
    return out_data


class StormCluster(object):
    """Object representing an interface to a Storm cluster.
    All the requests are basically translated into Storm commands.
    """
    def __init__(self, nimbus_ip, verbose):
        """Create the cluster object"""

        if nimbus_ip is None:
            raise ConfigurationError(
                "You must specify a storm cluster IP address."
                " Use option <storm_cluster_ip> in the configuration file"
                " or the command line option --storm-cluster")
        _validate_ip_address(nimbus_ip)

        self.nimbus_ip = nimbus_ip
        self.verbose = verbose

    def submit(self, jar_path):
        """Submit the pyleus topology jar to the Storm cluster"""
        cmd = ["jar", jar_path, TOPOLOGY_BUILDER_CLASS]

        _exec_storm_cmd(cmd, self.nimbus_ip, self.verbose)

    def list(self):
        """List the topologies running on the Storm cluster"""
        cmd = ["list"]

        # No point in calling it without output
        _exec_storm_cmd(cmd, self.nimbus_ip, True)

    def kill(self, topology_name, wait_time):
        """Kill a topology running on the Storm cluster"""

        cmd = ["kill", topology_name]

        if wait_time is not None:
            cmd += ["-w", wait_time]

        _exec_storm_cmd(cmd, self.nimbus_ip, self.verbose)


class LocalStormCluster(object):
    """Object representing an interface to a local Storm cluster.
    All the requests are basically translated into Storm commands.
    """

    def run(self, jar_path):
        """Run locally a pyleus topology jar

        Note: In order to trigger the local mode for the selcted topology,
        PyleusTopologyBuilder needs to be called with the option <--local>.
        """

        cmd = ["jar", jar_path, TOPOLOGY_BUILDER_CLASS, LOCAL_OPTION]

        # Having no feedback from Storm misses the point of running a topology
        # locally, so verbosity should always be activated
        _exec_storm_cmd(cmd, None, True)
