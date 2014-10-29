import os

from pyleus.testing import mock
import pyleus.utils as utils


class TestUtils(object):

    @mock.patch.object(os, 'path', autospec=True)
    def test_expand_path(self, mock_path):
        mock_path.abspath.return_value = "bar"
        expanded = utils.expand_path("foo")
        mock_path.abspath.assert_has_calls([
            mock.call(mock_path.expanduser("foo"))])
        assert expanded == "bar"
