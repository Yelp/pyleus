import os
import mock

import testify as T

import pyleus.utils as utils

class UtilsTest(T.TestCase):

    @mock.patch.object(os, 'path', autospec=True)
    def test_expand_path(self, mock_path):
        mock_path.abspath.return_value = "bar"
        expanded = utils.expand_path("foo")
        mock_path.abspath.assert_has_calls([
            mock.call(mock_path.expanduser("foo"))])
        T.assert_equals(expanded, "bar")


if __name__ == '__main__':
        T.run()
