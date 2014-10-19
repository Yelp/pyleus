import os

import mock
from mock import sentinel
import pytest

from pyleus.cli.storm_cluster import _get_storm_cmd_env
from pyleus.cli.storm_cluster import STORM_JAR_JVM_OPTS
from pyleus.cli.storm_cluster import StormCluster
from pyleus.cli.storm_cluster import TOPOLOGY_BUILDER_CLASS


class TestGetStormCmdEnd(object):

    @pytest.fixture(autouse=True)
    def mock_os_environ(self, monkeypatch):
        monkeypatch.setattr(os, 'environ', {})

    def test_jvm_opts_unset(self):
        assert _get_storm_cmd_env(None) is None

    def test_jvm_opts_set(self):
        jvm_opts = "-Dfoo=bar"
        env = _get_storm_cmd_env(jvm_opts)
        assert env[STORM_JAR_JVM_OPTS] == jvm_opts


class TestStormCluster(object):

    @pytest.fixture
    def cluster(self):
        return StormCluster(
            sentinel.storm_cmd_path,
            sentinel.nimbus_host,
            sentinel.nimbus_port,
            sentinel.verbose,
            sentinel.jvm_opts,
        )

    def test__build_storm_cmd_no_port(self, cluster):
        cluster.nimbus_host = "test-host"
        cluster.nimbus_port = None
        storm_cmd = cluster._build_storm_cmd(["a", "cmd"])
        assert storm_cmd == [sentinel.storm_cmd_path, "a", "cmd", "-c",
                             "nimbus.host=test-host"]

    def test__build_storm_cmd_with_port(self, cluster):
        cluster.nimbus_host = "test-host"
        cluster.nimbus_port = 4321
        storm_cmd = cluster._build_storm_cmd(["another", "cmd"])
        assert storm_cmd == [sentinel.storm_cmd_path, "another", "cmd", "-c",
                             "nimbus.host=test-host", "-c",
                             "nimbus.thrift.port=4321"]

    def test_submit(self, cluster):
        with mock.patch.object(cluster, '_exec_storm_cmd') as mock_exec:
            cluster.submit(sentinel.jar_path)

        mock_exec.assert_called_once_with(["jar", sentinel.jar_path, TOPOLOGY_BUILDER_CLASS])
