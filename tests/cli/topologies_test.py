import mock
from mock import sentinel
import pytest

from pyleus.configuration import Configuration, DEFAULTS
import pyleus.cli.topologies
from pyleus.cli.topologies import kill_topology
from pyleus.cli.topologies import list_topologies
from pyleus.cli.topologies import submit_topology


@pytest.fixture
def configs():
    """Create a mock Configuration object with sentinel values

    Eg.

        Configuration(
            base_jar=sentinel.base_jar,
            config_file=sentinel.config_file,
            ...
        )
    """
    return Configuration(**dict(
        (k, getattr(sentinel, k))
        for k in DEFAULTS._asdict().keys()
    ))


def test_submit_topology(configs):
    mock_storm_cluster = mock.Mock()

    with mock.patch.object(pyleus.cli.topologies, 'StormCluster',
            return_value=mock_storm_cluster) as mock_ctr:
        submit_topology(sentinel.jar_path, configs)

    mock_ctr.assert_called_once_with(
        configs.storm_cmd_path,
        configs.nimbus_host,
        configs.nimbus_port,
        configs.verbose,
        configs.jvm_opts,
    )

    mock_storm_cluster.submit.assert_called_once_with(sentinel.jar_path)


def test_kill_topology(configs):
    mock_storm_cluster = mock.Mock()

    with mock.patch.object(pyleus.cli.topologies, 'StormCluster',
            return_value=mock_storm_cluster) as mock_ctr:
        kill_topology(configs)

    mock_ctr.assert_called_once_with(
        configs.storm_cmd_path,
        configs.nimbus_host,
        configs.nimbus_port,
        configs.verbose,
        configs.jvm_opts,
    )

    mock_storm_cluster.kill.assert_called_once_with(configs.topology_name, configs.wait_time)


def test_list_topologies(configs):
    mock_storm_cluster = mock.Mock()

    with mock.patch.object(pyleus.cli.topologies, 'StormCluster',
            return_value=mock_storm_cluster) as mock_ctr:
        list_topologies(configs)

    mock_ctr.assert_called_once_with(
        configs.storm_cmd_path,
        configs.nimbus_host,
        configs.nimbus_port,
        configs.verbose,
        configs.jvm_opts,
    )

    mock_storm_cluster.list.assert_called_once_with()
