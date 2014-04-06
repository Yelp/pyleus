import contextlib
from cStringIO import StringIO

import mock
import testify as T

try:
    import simplejson as json
    _ = json # pyflakes
except ImportError:
    import json

from pyleus.storm import StormTuple, Bolt, Spout, is_tick
from pyleus.testing import StormComponentTestCase


class StormComponentTest(StormComponentTestCase):

    def test__read_msg_dict(self):
        msg_dict = {
            'hello': "world",
        }

        self.mock_input_stream.readline.side_effect = [
            json.dumps(msg_dict),
            "end",
        ]

        T.assert_equal(self.instance._read_msg(), msg_dict)

    def test__read_msg_list(self):
        msg_list = [3, 4, 5]

        self.mock_input_stream.readline.side_effect = [
            json.dumps(msg_list),
            "end",
        ]

        T.assert_equal(self.instance._read_msg(), msg_list)

    def test__msg_is_command(self):
        command_msg = dict(this_is_a_command=True)
        taskid_msg = ["this", "is", "a", "taskid", "list"]

        T.assert_equal(self.instance._msg_is_command(command_msg), True)
        T.assert_equal(self.instance._msg_is_command(taskid_msg), False)

    def test__msg_is_taskid(self):
        command_msg = dict(this_is_a_command=True)
        taskid_msg = ["this", "is", "a", "taskid", "list"]

        T.assert_equal(self.instance._msg_is_taskid(command_msg), False)
        T.assert_equal(self.instance._msg_is_taskid(taskid_msg), True)

    def test_read_command(self):
        command_msg = dict(this_is_a_command=True)
        taskid_msg = ["this", "is", "a", "taskid", "list"]

        messages = [
            taskid_msg,
            taskid_msg,
            taskid_msg,
            command_msg,
        ]

        with mock.patch.object(self.instance, '_read_msg',
                side_effect=messages):
            command = self.instance.read_command()

        T.assert_equal(command, command_msg)
        T.assert_equal(len(self.instance._pending_taskids), 3)

    def test_read_command_queued(self):
        next_command = dict(next_command=3)
        another_command = dict(another_command=7)

        self.instance._pending_commands.extend([
            next_command,
            another_command,
            another_command,
        ])

        T.assert_equal(self.instance.read_command(), next_command)
        T.assert_equal(len(self.instance._pending_commands), 2)

    def test_read_taskid(self):
        command_msg = dict(this_is_a_command=True)
        taskid_msg = ["this", "is", "a", "taskid", "list"]

        messages = [
            command_msg,
            command_msg,
            command_msg,
            taskid_msg,
        ]

        with mock.patch.object(self.instance, '_read_msg',
                side_effect=messages):
            taskid = self.instance.read_taskid()

        T.assert_equal(taskid, taskid_msg)
        T.assert_equal(len(self.instance._pending_commands), 3)

    def test_read_taskid_queued(self):
        next_taskid = dict(next_taskid=3)
        another_taskid = dict(another_taskid=7)

        self.instance._pending_taskids.extend([
            next_taskid,
            another_taskid,
            another_taskid,
        ])


        T.assert_equal(self.instance.read_taskid(), next_taskid)
        T.assert_equal(len(self.instance._pending_taskids), 2)

    def test_read_tuple(self):
        command_dict = {
            'id': "id",
            'comp': "comp",
            'stream': "stream",
            'task': "task",
            'tuple': "tuple",
        }

        expected_storm_tuple = StormTuple("id", "comp", "stream", "task",
            "tuple")

        with mock.patch.object(self.instance, 'read_command',
                return_value=command_dict):
            storm_tuple = self.instance.read_tuple()

        T.assert_isinstance(storm_tuple, StormTuple)
        T.assert_equal(storm_tuple, expected_storm_tuple)

    def test__send_msg(self):
        msg_dict = {
            'hello': "world",
        }

        expected_output = """{"hello": "world"}\nend\n"""

        with mock.patch.object(self.instance, '_output_stream',
                StringIO()) as sio:
            self.instance._send_msg(msg_dict)

        T.assert_equal(sio.getvalue(), expected_output)

    def test__create_pidfile(self):
        with mock.patch('__builtin__.open') as mock_open:
            self.instance._create_pidfile("pid_dir", "pid")

        mock_open.assert_called_once_with("pid_dir/pid", 'a')

    def test__init_component(self):
        handshake_msg = {
            'conf': {"foo": "bar"},
            'context': "context",
            'pidDir': "pidDir",
        }

        patch__read_msg = mock.patch.object(self.instance, '_read_msg',
            return_value=handshake_msg)
        patch_os_getpid = mock.patch('os.getpid', return_value=1234)
        patch__send_msg = mock.patch.object(self.instance, '_send_msg')
        patch__create_pidfile = mock.patch.object(self.instance,
            '_create_pidfile')

        patches = contextlib.nested(patch__read_msg, patch_os_getpid,
            patch__send_msg, patch__create_pidfile)

        with patches as (_, _, mock__send_msg, mock__create_pidfile):
            conf, context = self.instance._init_component()

        mock__send_msg.assert_called_once_with({'pid': 1234})
        mock__create_pidfile.assert_called_once_with("pidDir", 1234)

        T.assert_equal(conf, {"foo": "bar"})
        T.assert_equal(context, "context")

    def test_send_command_with_opts(self):
        with mock.patch.object(self.instance, '_send_msg') as mock__send_msg:
            self.instance.send_command('test', {'option': "foo"})

        mock__send_msg.assert_called_once_with({
            'command': "test",
            'option': "foo",
        })

    def test_send_command_with_no_opts(self):
        with mock.patch.object(self.instance, '_send_msg') as mock__send_msg:
            self.instance.send_command('test')

        mock__send_msg.assert_called_once_with({
            'command': "test",
        })

    def test_send_command_clobber_command(self):
        with mock.patch.object(self.instance, '_send_msg') as mock__send_msg:
            self.instance.send_command('test', {'command': "joe"})

        mock__send_msg.assert_called_once_with({
            'command': "test",
        })


class BoltTest(StormComponentTestCase):

    INSTANCE_CLS = Bolt

    def test_ack(self):
        tup = mock.Mock(id=1234)

        with mock.patch.object(self.instance, 'send_command') as \
                mock_send_command:
            self.instance.ack(tup)

        mock_send_command.assert_called_once_with('ack', {'id': tup.id})

    def test_fail(self):
        tup = mock.Mock(id=1234)

        with mock.patch.object(self.instance, 'send_command') as \
                mock_send_command:
            self.instance.fail(tup)

        mock_send_command.assert_called_once_with('fail', {'id': tup.id})

    @contextlib.contextmanager
    def _test_emit_helper(self, expected_command_dict):
        patches = contextlib.nested(
            mock.patch.object(self.instance, 'read_taskid'),
            mock.patch.object(self.instance, 'send_command'),
        )

        with patches as (mock_read_taskid, mock_send_command):
            yield

        mock_read_taskid.assert_called_once_with()
        mock_send_command.assert_called_once_with('emit', expected_command_dict)

    def test_emit_simple(self):
        expected_command_dict = {
            'anchors': [],
            'tuple': (1, 2, 3),
        }

        with self._test_emit_helper(expected_command_dict):
            self.instance.emit((1, 2, 3))

    def test_emit_with_list(self):
        expected_command_dict = {
            'anchors': [],
            'tuple': [1, 2, 3],
        }

        with self._test_emit_helper(expected_command_dict):
            self.instance.emit([1, 2, 3])

    def test_emit_with_stream(self):
        expected_command_dict = {
            'anchors': [],
            'stream': mock.sentinel.stream,
            'tuple': (1, 2, 3),
        }

        with self._test_emit_helper(expected_command_dict):
            self.instance.emit((1, 2, 3), stream=mock.sentinel.stream)

    def test_emit_with_anchors(self):
        expected_command_dict = {
            'anchors': [4, 5, 6],
            'tuple': (1, 2, 3),
        }

        anchors = [mock.Mock(id=i) for i in (4, 5, 6)]

        with self._test_emit_helper(expected_command_dict):
            self.instance.emit((1, 2, 3), anchors=anchors)

    def test_emit_with_direct_task(self):
        expected_command_dict = {
            'anchors': [],
            'task': mock.sentinel.direct_task,
            'tuple': (1, 2, 3),
        }

        with self._test_emit_helper(expected_command_dict):
            self.instance.emit((1, 2, 3), direct_task=mock.sentinel.direct_task)

    def test_emit_with_bad_values(self):
        T.assert_raises(AssertionError, self.instance.emit,
            "not-a-list-or-tuple")


class SpoutTest(StormComponentTestCase):

    INSTANCE_CLS = Spout

    def test__sync(self):
        with mock.patch.object(self.instance, 'send_command') \
               as mock_send_command:
            self.instance._sync()

        mock_send_command.assert_called_once_with('sync')

    @contextlib.contextmanager
    def _test_emit_helper(self, expected_command_dict):
        patches = contextlib.nested(
            mock.patch.object(self.instance, 'read_taskid'),
            mock.patch.object(self.instance, 'send_command'),
        )

        with patches as (mock_read_taskid, mock_send_command):
            yield

        mock_read_taskid.assert_called_once_with()
        mock_send_command.assert_called_once_with('emit', expected_command_dict)

    def test_emit_simple(self):
        expected_command_dict = {
            'tuple': (1, 2, 3),
        }

        with self._test_emit_helper(expected_command_dict):
            self.instance.emit((1, 2, 3))

    def test_emit_with_stream(self):
        expected_command_dict = {
            'stream': mock.sentinel.stream,
            'tuple': (1, 2, 3),
        }

        with self._test_emit_helper(expected_command_dict):
            self.instance.emit((1, 2, 3), stream=mock.sentinel.stream)

    def test_emit_with_tup_id(self):
        expected_command_dict = {
            'id': mock.sentinel.tup_id,
            'tuple': (1, 2, 3),
        }

        with self._test_emit_helper(expected_command_dict):
            self.instance.emit((1, 2, 3), tup_id=mock.sentinel.tup_id)

    def test_emit_with_direct_task(self):
        expected_command_dict = {
            'task': mock.sentinel.direct_task,
            'tuple': (1, 2, 3),
        }

        with self._test_emit_helper(expected_command_dict):
            self.instance.emit((1, 2, 3), direct_task=mock.sentinel.direct_task)

    def test__handle_command_next(self):
        msg = dict(command='next')
        with mock.patch.object(self.instance, 'next_tuple') as mock_next_tuple:
            self.instance._handle_command(msg)

        mock_next_tuple.assert_called_once_with()

    def test__handle_command_ack(self):
        msg = dict(command='ack', id=mock.sentinel.tuple_id)
        with mock.patch.object(self.instance, 'ack') as mock_ack:
            self.instance._handle_command(msg)

        mock_ack.assert_called_once_with(mock.sentinel.tuple_id)

    def test__handle_command_fail(self):
        msg = dict(command='fail', id=mock.sentinel.tuple_id)
        with mock.patch.object(self.instance, 'fail') as mock_fail:
            self.instance._handle_command(msg)

        mock_fail.assert_called_once_with(mock.sentinel.tuple_id)


class StormUtilFunctionTestCase(T.TestCase):

    def test_is_tick_true(self):
        tup = StormTuple(None, '__system', '__tick', None, None)
        T.assert_equal(is_tick(tup), True)

    def test_is_tick_false(self):
        tup = StormTuple(None, '__system', None, None, None)
        T.assert_equal(is_tick(tup), False)
        tup = StormTuple(None, None, '__tick', None, None)
        T.assert_equal(is_tick(tup), False)
