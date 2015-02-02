from collections import namedtuple
import contextlib

import pytest

from pyleus.storm import StormTuple, Bolt, SimpleBolt
from pyleus.testing import ComponentTestCase, mock


class TestBolt(ComponentTestCase):

    INSTANCE_CLS = Bolt

    def test_ack(self):
        tup = mock.Mock(id=1234)

        with mock.patch.object(self.instance, 'send_command', autospec=True) as \
                mock_send_command:
            self.instance.ack(tup)

        mock_send_command.assert_called_once_with('ack', {'id': tup.id})

    def test_heartbeat(self):
        heartbeat = StormTuple(None, None, '__heartbeat', -1, [])

        with mock.patch.multiple(self.instance,
                process_tuple=mock.DEFAULT,
                ack=mock.DEFAULT,
                send_command=mock.DEFAULT) as values:

            self.instance._process_tuple(heartbeat)

            values['send_command'].assert_called_once_with('sync')
            assert not values['process_tuple'].called
            assert not values['ack'].called

    def test_fail(self):
        tup = mock.Mock(id=1234)

        with mock.patch.object(self.instance, 'send_command', autospec=True) as \
                mock_send_command:
            self.instance.fail(tup)

        mock_send_command.assert_called_once_with('fail', {'id': tup.id})

    @contextlib.contextmanager
    def _test_emit_helper(self, expected_command_dict):
        with mock.patch.object(self.instance, 'read_taskid', autospec=True) as mock_read_taskid:
            with mock.patch.object(self.instance, 'send_command', autospec=True) as mock_send_command:
                yield mock_send_command

        mock_read_taskid.assert_called_once_with()
        mock_send_command.assert_called_once_with('emit', expected_command_dict)

    @contextlib.contextmanager
    def _test_emit_helper_no_taskid(self, expected_command_dict):
        with mock.patch.object(self.instance, 'read_taskid', autospec=True) as mock_read_taskid:
            with mock.patch.object(self.instance, 'send_command', autospec=True) as mock_send_command:
                yield mock_send_command

        assert mock_read_taskid.call_count == 0
        mock_send_command.assert_called_once_with('emit', expected_command_dict)

    def test_emit_simple(self):
        expected_command_dict = {
            'anchors': [],
            'tuple': (1, 2, 3),
        }

        with self._test_emit_helper(expected_command_dict):
            self.instance.emit((1, 2, 3))

    def test_emit_simple_no_taskid(self):
        expected_command_dict = {
            'anchors': [],
            'tuple': (1, 2, 3),
            'need_task_ids': False,
        }

        with self._test_emit_helper_no_taskid(expected_command_dict):
            self.instance.emit((1, 2, 3), need_task_ids=False)

    def test_emit_with_list(self):
        expected_command_dict = {
            'anchors': [],
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
            'anchors': [],
            'tuple': tuple(values),
        }

        with self._test_emit_helper(expected_command_dict) as mock_send_command:
            self.instance.emit(values)

        _, command_dict = mock_send_command.call_args[0]
        assert command_dict['tuple'].__class__ == tuple

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
        with pytest.raises(AssertionError):
            self.instance.emit("not-a-list-or-tuple")


class TestSimpleBolt(ComponentTestCase):

    INSTANCE_CLS = SimpleBolt

    TICK = StormTuple(None, '__system', '__tick', None, None)
    HEARTBEAT = StormTuple(None, None, '__heartbeat', -1, [])
    TUPLE = StormTuple(None, None, None, None, None)

    @pytest.fixture(autouse=True)
    def setup_mocks(self, request):
        patches = mock.patch.multiple(self.instance, process_tick=mock.DEFAULT,
                                      process_tuple=mock.DEFAULT, ack=mock.DEFAULT,
                                      sync=mock.DEFAULT)

        request.addfinalizer(lambda: patches.__exit__(None, None, None))
        values = patches.__enter__()
        self.mock_process_tick = values['process_tick']
        self.mock_process_tuple = values['process_tuple']
        self.mock_ack = values['ack']
        self.mock_sync = values['sync']

    def test_ack(self):
        self.instance._process_tuple(self.TICK)
        self.instance._process_tuple(self.TUPLE)

        self.mock_ack.assert_has_calls([
            mock.call(self.TICK),
            mock.call(self.TUPLE),
        ])

    def test_tick(self):
        self.instance._process_tuple(self.TICK)

        self.mock_process_tick.assert_called_once_with()
        assert not self.mock_process_tuple.called

    def test_heartbeat(self):
        self.instance._process_tuple(self.HEARTBEAT)

        self.mock_sync.assert_called_once_with()
        assert not self.mock_process_tuple.called
        assert not self.mock_ack.called

    def test_tuple(self):
        self.instance._process_tuple(self.TUPLE)

        assert not self.mock_process_tick.called
        self.mock_process_tuple.assert_called_once_with(self.TUPLE)

    def test_exception(self):
        class MyException(Exception): pass
        self.mock_process_tick.side_effect = MyException()
        self.mock_process_tuple.side_effect = MyException()

        with pytest.raises(MyException):
            self.instance._process_tuple(self.TICK)

        with pytest.raises(MyException):
            self.instance._process_tuple(self.TUPLE)
