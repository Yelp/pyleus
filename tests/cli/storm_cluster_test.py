import os

import pytest

from pyleus.cli.storm_cluster import _get_storm_cmd_env, STORM_JAR_JVM_OPTS


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
