from collections import namedtuple
import contextlib

import mock
from mock import call, sentinel
import pytest

from pyleus.storm import StormTuple, Bolt, SimpleBolt
from pyleus.testing import ComponentTestCase


class TestBolt(ComponentTestCase):

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
            yield mock_send_command

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
            'stream': sentinel.stream,
            'tuple': (1, 2, 3),
        }

        with self._test_emit_helper(expected_command_dict):
            self.instance.emit((1, 2, 3), stream=sentinel.stream)

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
            'task': sentinel.direct_task,
            'tuple': (1, 2, 3),
        }

        with self._test_emit_helper(expected_command_dict):
            self.instance.emit((1, 2, 3), direct_task=sentinel.direct_task)

    def test_emit_with_bad_values(self):
        with pytest.raises(AssertionError):
            self.instance.emit("not-a-list-or-tuple")


class TestSimpleBolt(ComponentTestCase):

    INSTANCE_CLS = SimpleBolt

    TICK = StormTuple(None, '__system', '__tick', None, None)
    TUPLE = StormTuple(None, None, None, None, None)

    @pytest.fixture(autouse=True)
    def setup_mocks(self, request):
        patches = contextlib.nested(
            mock.patch.object(self.instance, 'process_tick'),
            mock.patch.object(self.instance, 'process_tuple'),
            mock.patch.object(self.instance, 'fail'),
            mock.patch.object(self.instance, 'ack'),
        )

        request.addfinalizer(lambda: patches.__exit__(None, None, None))
        (self.mock_process_tick, self.mock_process_tuple,
                         self.mock_fail, self.mock_ack) = patches.__enter__()

    def test_fail(self):
        self.mock_process_tick.side_effect = Exception()
        self.mock_process_tuple.side_effect = Exception()

        try:
            self.instance._process_tuple(self.TUPLE)
        except:
            # Exception expected
            pass

        try:
            self.instance._process_tuple(self.TICK)
        except:
            # Exception expected
            pass

        self.mock_fail.assert_has_calls([
            call(self.TUPLE),
            call(self.TICK),
        ])

        assert not self.mock_ack.called

    def test_ack(self):
        self.instance._process_tuple(self.TICK)
        self.instance._process_tuple(self.TUPLE)

        assert not self.mock_fail.called

        self.mock_ack.assert_has_calls([
            call(self.TICK),
            call(self.TUPLE),
        ])

    def test_tick(self):
        self.instance._process_tuple(self.TICK)

        self.mock_process_tick.assert_called_once_with()
        assert not self.mock_process_tuple.called

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
