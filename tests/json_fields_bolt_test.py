import mock
import testify as T

from pyleus.json_fields_bolt import JSONFieldsBolt
from pyleus.testing import ComponentTestCase


class JSONFieldsBoltTest(ComponentTestCase):

    INSTANCE_CLS = JSONFieldsBolt

    @T.setup_teardown
    def setup_teardown(self):
        with mock.patch.object(self.instance, 'emit') as self.mock_emit:
            yield

    def test_process_tuple(self):
        line = """{"a": 1, "b": 2, "c": 3}"""

        def mock_extract_fields(d):
            return d['a'], d['b'], d['c']

        expected_values = (1, 2, 3)

        mock_tup = mock.Mock(values=(line,))

        with mock.patch.object(self.instance, 'extract_fields',
                mock_extract_fields):
            self.instance.process_tuple(mock_tup)

        self.mock_emit.assert_called_once_with(expected_values,
            anchors=[mock_tup])

    def test_no_emit_on_none(self):
        line = "{}"
        mock_tup = mock.Mock(values=(line,))

        with mock.patch.object(self.instance, 'extract_fields', side_effect=[None]):
            self.instance.process_tuple(mock_tup)

        T.assert_equal(self.mock_emit.called, False)
