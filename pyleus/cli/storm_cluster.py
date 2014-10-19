"""Interface classes to Storm clusters. As things are, they implement
cluster-topologies interactions, but not cluster management.
"""
from __future__ import absolute_import

import os
import signal
import subprocess

from pyleus.exception import ConfigurationError
from pyleus.exception import StormError


TOPOLOGY_BUILDER_CLASS = "com.yelp.pyleus.PyleusTopologyBuilder"
LOCAL_OPTION = "--local"
DEBUG_OPTION = "--debug"
STORM_JAR_JVM_OPTS = "STORM_JAR_JVM_OPTS"


def _watch_over_storm(storm_pid):
    """Ensure that if the pyleus process is killed, also the storm process
    will terminate.
    """
    def _kill_storm_handler(signum, frame):
        # Killing the storm process is enough for killing all python
        # subprocesses
        os.kill(storm_pid, signal.SIGTERM)

    signal.signal(signal.SIGTERM, _kill_storm_handler)
    signal.signal(signal.SIGINT, _kill_storm_handler)


def _get_storm_cmd_env(jvm_opts):
    """Return a copy of os.environ containing JVM options from the user.

    If no JVM options were specified, return None to defer to the default
    behavior of subprocess.Popen.
    """
    if jvm_opts:
        env = os.environ.copy()
        env[STORM_JAR_JVM_OPTS] = jvm_opts
        return env

    return None


class StormCluster(object):
    """Object representing an interface to a Storm cluster.
    All the requests are basically translated into Storm commands.
    """

    def __init__(self, storm_cmd_path, nimbus_host, nimbus_port, verbose,
                 jvm_opts):
        """Create the cluster object."""

        self.storm_cmd_path = storm_cmd_path

        if nimbus_host is None:
            raise ConfigurationError(
                "You must specify the Nimbus host. Use the option "
                "<nimbus_host> in the configuration file or the command line "
                "option -n/--nimbus-host.")

        self.nimbus_host = nimbus_host
        self.nimbus_port = nimbus_port
        self.verbose = verbose
        self.jvm_opts = jvm_opts

    def _build_storm_cmd(self, cmd):
        storm_cmd = [self.storm_cmd_path]
        storm_cmd += cmd
        storm_cmd += ["-c", "nimbus.host={0}".format(self.nimbus_host)]

        if self.nimbus_port:
            storm_cmd += ["-c", "nimbus.thrift.port={0}".format(
                self.nimbus_port)]

        return storm_cmd

    def _exec_storm_cmd(self, cmd, verbose=None):
        """Interface to any Storm command."""
        storm_cmd = self._build_storm_cmd(cmd)

        out_stream = None
        if verbose is None:
            verbose = self.verbose
        if not verbose:
            out_stream = open(os.devnull, "w")

        env = _get_storm_cmd_env(self.jvm_opts)

        proc = subprocess.Popen(storm_cmd,
                                stdout=out_stream,
                                stderr=subprocess.STDOUT,
                                env=env)
        out_data, _ = proc.communicate()
        if proc.returncode != 0:
            raise StormError(
                "Storm command failed. Run with --verbose for more info.")
        return out_data

    def submit(self, jar_path):
        """Submit the pyleus topology jar to the Storm cluster."""
        cmd = ["jar", jar_path, TOPOLOGY_BUILDER_CLASS]

        self._exec_storm_cmd(cmd)

    def list(self):
        """List the topologies running on the Storm cluster."""
        cmd = ["list"]

        # No point in calling it without output
        self._exec_storm_cmd(cmd, True)

    def kill(self, topology_name, wait_time):
        """Kill a topology running on the Storm cluster."""

        cmd = ["kill", topology_name]

        if wait_time is not None:
            cmd += ["-w", wait_time]

        self._exec_storm_cmd(cmd)


class LocalStormCluster(object):
    """Object representing an interface to a local Storm cluster.
    All the requests are basically translated into Storm commands.
    """

    def run(self, storm_cmd_path, jar_path, debug, jvm_opts):
        """Run locally a pyleus topology jar.

        Note: In order to trigger the local mode for the selcted topology,
        PyleusTopologyBuilder needs to be called with the option <--local>.
        """
        storm_cmd = [
            storm_cmd_path,
            "jar",
            jar_path,
            TOPOLOGY_BUILDER_CLASS,
            LOCAL_OPTION
        ]

        if debug:
            storm_cmd.append(DEBUG_OPTION)

        env = _get_storm_cmd_env(jvm_opts)

        # Having no feedback from Storm misses the point of running a topology
        # locally, so output is always redirected to stdout
        proc = subprocess.Popen(storm_cmd, env=env)

        _watch_over_storm(proc.pid)

        proc.communicate()
        if proc.returncode != 0:
            raise StormError(
                "Storm command failed. Run with --verbose for more info.")
