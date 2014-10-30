import logging.config
import os.path

from pyleus.storm import StormTuple
from pyleus.storm.component import DEFAULT_LOGGING_CONFIG_PATH
from pyleus.storm.serializers.serializer import Serializer
from pyleus.testing import ComponentTestCase, mock, builtins


class TestComponent(ComponentTestCase):

    def test__msg_is_command(self):
        command_msg = dict(this_is_a_command=True)
        taskid_msg = ["this", "is", "a", "taskid", "list"]

        assert self.instance._msg_is_command(command_msg)
        assert not self.instance._msg_is_command(taskid_msg)

    def test__msg_is_taskid(self):
        command_msg = dict(this_is_a_command=True)
        taskid_msg = ["this", "is", "a", "taskid", "list"]

        assert not self.instance._msg_is_taskid(command_msg)
        assert self.instance._msg_is_taskid(taskid_msg)

    def test_read_command(self):
        command_msg = dict(this_is_a_command=True)
        taskid_msg = ["this", "is", "a", "taskid", "list"]

        messages = [
            taskid_msg,
            taskid_msg,
            taskid_msg,
            command_msg,
        ]

        with mock.patch.object(
                self.instance, '_serializer', autospec=Serializer):
            self.instance._serializer.read_msg.side_effect = messages
            command = self.instance.read_command()

        assert command == command_msg
        assert len(self.instance._pending_taskids) == 3

    def test_read_command_queued(self):
        next_command = dict(next_command=3)
        another_command = dict(another_command=7)

        self.instance._pending_commands.extend([
            next_command,
            another_command,
            another_command,
        ])

        assert self.instance.read_command() == next_command
        assert len(self.instance._pending_commands) == 2

    def test_read_taskid(self):
        command_msg = dict(this_is_a_command=True)
        taskid_msg = ["this", "is", "a", "taskid", "list"]

        messages = [
            command_msg,
            command_msg,
            command_msg,
            taskid_msg,
        ]

        with mock.patch.object(
                self.instance, '_serializer', autospec=Serializer):
            self.instance._serializer.read_msg.side_effect = messages
            taskid = self.instance.read_taskid()

        assert taskid == taskid_msg
        assert len(self.instance._pending_commands) == 3

    def test_read_taskid_queued(self):
        next_taskid = dict(next_taskid=3)
        another_taskid = dict(another_taskid=7)

        self.instance._pending_taskids.extend([
            next_taskid,
            another_taskid,
            another_taskid,
        ])

        assert self.instance.read_taskid() == next_taskid
        assert len(self.instance._pending_taskids) == 2

    def test_read_tuple(self):
        command_dict = {
            'id': "id",
            'comp': "comp",
            'stream': "stream",
            'task': "task",
            'tuple': "tuple",
        }

        expected_storm_tuple = StormTuple(
            "id", "comp", "stream", "task", "tuple")

        with mock.patch.object(
                self.instance, 'read_command', return_value=command_dict):
            storm_tuple = self.instance.read_tuple()

        assert isinstance(storm_tuple, StormTuple)
        assert storm_tuple == expected_storm_tuple

    def test__create_pidfile(self):
        with mock.patch.object(builtins, 'open', autospec=True) as mock_open:
            self.instance._create_pidfile("pid_dir", "pid")

        mock_open.assert_called_once_with("pid_dir/pid", 'a')

    def test__init_component(self):
        handshake_msg = {
            'conf': {"foo": "bar"},
            'context': "context",
            'pidDir': "pidDir",
        }

        patch_serializer = mock.patch.object(
            self.instance, '_serializer', autospec=Serializer)
        patch_os_getpid = mock.patch('os.getpid', return_value=1234)
        patch__create_pidfile = mock.patch.object(
            self.instance, '_create_pidfile')

        with patch_serializer as mock_serializer:
            with patch_os_getpid as mock_os_getpid:
                with patch__create_pidfile as mock__create_pidfile:
                    mock_serializer.read_msg.return_value = handshake_msg
                    conf, context = self.instance._init_component()

        mock_os_getpid.assert_called_once_with()
        mock_serializer.send_msg.assert_called_once_with({'pid': 1234})
        mock__create_pidfile.assert_called_once_with("pidDir", 1234)

        assert conf == {"foo": "bar"}
        assert context == "context"

    def test_send_command_with_opts(self):
        with mock.patch.object(
                self.instance, '_serializer', autospec=Serializer):
            self.instance.send_command('test', {'option': "foo"})

            self.instance._serializer.send_msg.assert_called_once_with({
                'command': "test",
                'option': "foo",
            })

    def test_send_command_with_no_opts(self):
        with mock.patch.object(
                self.instance, '_serializer', autospec=Serializer):
            self.instance.send_command('test')

            self.instance._serializer.send_msg.assert_called_once_with({
                'command': "test",
            })

    def test_send_command_clobber_command(self):
        with mock.patch.object(
                self.instance, '_serializer', autospec=Serializer):
            self.instance.send_command('test', {'command': "joe"})

            self.instance._serializer.send_msg.assert_called_once_with({
                'command': "test",
            })

    @mock.patch.object(logging.config, 'fileConfig')
    def test_initialize_logging(self, fileConfig):
        pyleus_config = {
            'logging_config_path': mock.sentinel.logging_config_path}
        with mock.patch.object(self.instance, 'pyleus_config', pyleus_config):
            self.instance.initialize_logging()

        fileConfig.assert_called_once_with(mock.sentinel.logging_config_path)

    @mock.patch.object(logging.config, 'fileConfig')
    def test_initialize_logging_default_exists(self, fileConfig):
        with mock.patch.object(self.instance, 'pyleus_config', {}):
            with mock.patch.object(os.path, 'isfile', return_value=True):
                self.instance.initialize_logging()

        fileConfig.assert_called_once_with(DEFAULT_LOGGING_CONFIG_PATH)

    @mock.patch.object(logging.config, 'fileConfig')
    def test_initialize_logging_default_no_exists(self, fileConfig):
        with mock.patch.object(self.instance, 'pyleus_config', {}):
            with mock.patch.object(os.path, 'isfile', return_value=False):
                self.instance.initialize_logging()

        assert not fileConfig.called
