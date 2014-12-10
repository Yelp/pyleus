from collections import namedtuple
import contextlib

from pyleus.storm import Spout
from pyleus.testing import ComponentTestCase, mock


class TestSpout(ComponentTestCase):

    INSTANCE_CLS = Spout

    def test__sync(self):
        with mock.patch.object(self.instance, 'send_command', autospec=True) \
               as mock_send_command:
            self.instance._sync()

        mock_send_command.assert_called_once_with('sync')

    @contextlib.contextmanager
    def _test_emit_helper(self, expected_command_dict):
        with mock.patch.object(self.instance, 'read_taskid', autospec=True) as mock_read_taskid:
            with mock.patch.object(self.instance, 'send_command', autospec=True) as mock_send_command:
                yield mock_send_command

        mock_read_taskid.assert_called_once_with()
        mock_send_command.assert_called_once_with('emit', expected_command_dict)

    @contextlib.contextmanager
    def _test_emit_no_taskid_helper(self, expected_command_dict):
        with mock.patch.object(self.instance, 'read_taskid', autospec=True) as mock_read_taskid:
            with mock.patch.object(self.instance, 'send_command', autospec=True) as mock_send_command:
                yield mock_send_command

        assert mock_read_taskid.call_count == 0
        mock_send_command.assert_called_once_with('emit', expected_command_dict)

    def test_emit_simple(self):
        expected_command_dict = {
            'tuple': (1, 2, 3),
        }

        with self._test_emit_helper(expected_command_dict):
            self.instance.emit((1, 2, 3))

    def test_emit_simple_no_taskid(self):
        expected_command_dict = {
            'tuple': (1, 2, 3),
            'need_task_ids': False,
        }

        with self._test_emit_no_taskid_helper(expected_command_dict):
            self.instance.emit((1, 2, 3), need_task_ids=False)

    def test_emit_with_list(self):
        expected_command_dict = {
            'tuple': tuple([1, 2, 3]),
        }

        with self._test_emit_helper(expected_command_dict):
            self.instance.emit([1, 2, 3])

    def test_emit_with_namedtuple(self):
        """Regression test for PYLEUS-60

        Some versions of simplejson serialize namedtuples differently, so
        Pyleus casts all outgoing tuple values to actual Python tuples before
        emitting.
        """
        MyTuple = namedtuple('MyTuple', "a b c")
        values = MyTuple(1, 2, 3)

        expected_command_dict = {
            'tuple': tuple(values),
        }

        with self._test_emit_helper(expected_command_dict) as mock_send_command:
            self.instance.emit(values)

        _, command_dict = mock_send_command.call_args[0]
        assert command_dict['tuple'].__class__ == tuple

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
        with mock.patch.object(self.instance, 'next_tuple', autospec=True) as mock_next_tuple:
            self.instance._handle_command(msg)

        mock_next_tuple.assert_called_once_with()

    def test__handle_command_ack(self):
        msg = dict(command='ack', id=mock.sentinel.tuple_id)
        with mock.patch.object(self.instance, 'ack', autospec=True) as mock_ack:
            self.instance._handle_command(msg)

        mock_ack.assert_called_once_with(mock.sentinel.tuple_id)

    def test__handle_command_fail(self):
        msg = dict(command='fail', id=mock.sentinel.tuple_id)
        with mock.patch.object(self.instance, 'fail', autospec=True) as mock_fail:
            self.instance._handle_command(msg)

        mock_fail.assert_called_once_with(mock.sentinel.tuple_id)
