"""Interface classes to Storm clusters. As things are, they implement
cluster-topologies interactions, but not cluster management.
"""
from __future__ import absolute_import

import os
import signal
import socket
import subprocess

from pyleus.exception import ConfigurationError
from pyleus.exception import StormError


STORM_PATH = "/usr/share/storm/bin/storm"
TOPOLOGY_BUILDER_CLASS = "com.yelp.pyleus.PyleusTopologyBuilder"
LOCAL_OPTION = "--local"


def _watch_over_storm(storm_pid):
    """ Ensure that if the pyleus process is killed, also the storm process
    will terminate
    """
    def _kill_storm_handler(signum, frame):
        # Killing the storm process is enough for killing all python
        # subprocesses
        os.kill(storm_pid, signal.SIGTERM)

    signal.signal(signal.SIGTERM, _kill_storm_handler)
    signal.signal(signal.SIGINT, _kill_storm_handler)


def _validate_ip_address(address):
    """Ensure that the given address is valid.

    Note: IPv4 only.
    """
    try:
        socket.inet_aton(address)
    except socket.error:
        raise ConfigurationError("The IP address specified is invalid: {0}"
                                 .format(address))


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

    def _exec_storm_cmd(self, cmd, verbose=None):
        """Interface to any storm command"""
        out_stream = None
        if verbose is None:
            verbose = self.verbose
        if not verbose:
            out_stream = open(os.devnull, "w")

        storm_cmd = [STORM_PATH]
        storm_cmd += cmd
        storm_cmd += ["-c", "nimbus.host={0}".format(self.nimbus_ip)]

        proc = subprocess.Popen(storm_cmd,
                                stdout=out_stream,
                                stderr=subprocess.STDOUT)
        out_data, _ = proc.communicate()
        if proc.returncode != 0:
            raise StormError(
                "Storm command failed. Run with --verbose for more info.")
        return out_data

    def submit(self, jar_path):
        """Submit the pyleus topology jar to the Storm cluster"""
        cmd = ["jar", jar_path, TOPOLOGY_BUILDER_CLASS]

        self._exec_storm_cmd(cmd)

    def list(self):
        """List the topologies running on the Storm cluster"""
        cmd = ["list"]

        # No point in calling it without output
        self._exec_storm_cmd(cmd, True)

    def kill(self, topology_name, wait_time):
        """Kill a topology running on the Storm cluster"""

        cmd = ["kill", topology_name]

        if wait_time is not None:
            cmd += ["-w", wait_time]

        self._exec_storm_cmd(cmd)


class LocalStormCluster(object):
    """Object representing an interface to a local Storm cluster.
    All the requests are basically translated into Storm commands.
    """

    def run(self, jar_path):
        """Run locally a pyleus topology jar

        Note: In order to trigger the local mode for the selcted topology,
        PyleusTopologyBuilder needs to be called with the option <--local>.
        """
        storm_cmd = [
            STORM_PATH, "jar", jar_path, TOPOLOGY_BUILDER_CLASS, LOCAL_OPTION]

        # Having no feedback from Storm misses the point of running a topology
        # locally, so output is always redirected to stdout
        proc = subprocess.Popen(storm_cmd, stderr=subprocess.STDOUT)

        _watch_over_storm(proc.pid)

        proc.communicate()
        if proc.returncode != 0:
            raise StormError(
                "Storm command failed. Run with --verbose for more info.")
