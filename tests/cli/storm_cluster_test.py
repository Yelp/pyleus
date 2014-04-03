import os

import mock
import testify as T

from pyleus.cli.storm_cluster import _get_storm_cmd_env, STORM_JAR_JVM_OPTS


class GetStormCmdEndTest(T.TestCase):

    @T.setup_teardown
    def setup_mocks(self):
        with mock.patch.object(os, 'environ', {}):
            yield

    def test_jvm_opts_unset(self):
        T.assert_equal(_get_storm_cmd_env(None), None)

    def test_jvm_opts_set(self):
        jvm_opts = "-Dfoo=bar"
        env = _get_storm_cmd_env(jvm_opts)
        T.assert_equal(env[STORM_JAR_JVM_OPTS], jvm_opts)
