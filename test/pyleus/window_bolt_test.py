import contextlib

import mock
import numpy
import testify as T

from pyleus.window_bolt import Record, Window, WindowBolt

from .storm_test import StormComponentTestCase


def _fake_record(key=None, timestamp=None, value=None):
    if key is None:
        key = mock.sentinel.key

    if timestamp is None:
        timestamp = mock.sentinel.timestamp

    if value is None:
        value = mock.sentinel.value

    return Record(key, timestamp, value)


class WindowTest(T.TestCase):

    @T.setup
    def setup(self):
        self.instance = Window()

    def test_insert_record(self):
        self.instance.insert_record(mock.sentinel.record)
        T.assert_equal(self.instance.records[60], [mock.sentinel.record])
        T.assert_equal(self.instance.records[300], [mock.sentinel.record])

    def test_cull_old_events(self):
        original_records = [
            _fake_record(timestamp=ts)
            for ts in (1, 2, 3, 4, 5)
        ]

        mock_records = {
            60: list(original_records),
            300: list(original_records),
        }

        for window_size in (60, 300):
            with mock.patch.object(self.instance, 'records', mock_records):
                start_time = window_size + 2
                self.instance.cull_old_events(start_time)

                # Expect records with timestamp in (1, 2) to be culled
                T.assert_equal(mock_records[window_size], original_records[2:])

    def test_compute_stats(self):
        start_time = 300

        # Mock records as if cull_old_events has run up to start_time
        mock_records = {
            60: [
                _fake_record(timestamp=ts, value=ts)
                for ts in xrange(241, 301) # Exactly 60 records
            ],
            300: [
                _fake_record(timestamp=ts, value=ts)
                for ts in xrange(1, 301) # Exactly 300 records
            ],
        }

        def percentiles_for_records(records):
            arr = numpy.array([r.value for r in records])
            return numpy.percentile(arr, (50, 75, 95, 99))

        expected_percentiles = {
            60: percentiles_for_records(mock_records[60]),
            300: percentiles_for_records(mock_records[300]),
        }

        patches = contextlib.nested(
            mock.patch.object(self.instance, 'records', mock_records),
            mock.patch.object(self.instance, 'cull_old_events'),
        )

        with patches as (_, mock_cull_old_events):
            results = list(self.instance.compute_stats(start_time)) # Force generator

        mock_cull_old_events.assert_called_once_with(start_time)
        T.assert_equal(results, [
            (60, expected_percentiles[60]),
            (300, expected_percentiles[300]),
        ])


class WindowBoltTest(StormComponentTestCase):

    INSTANCE_CLS = WindowBolt

    def test_process_tuple(self):
        key = mock.sentinel.key
        mock_record = _fake_record(key=key)
        mock_tup = mock.Mock(values=tuple(mock_record))

        mock_windows = {
            key: mock.Mock(),
        }

        with mock.patch.object(self.instance, 'windows', mock_windows):
            with mock.patch.object(self.instance, 'ack'):
                self.instance.process_tuple(mock_tup)

        mock_windows[key].insert_record.assert_called_once_with(mock_record)
