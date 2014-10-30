"""Helper functions and classes for testing."""
from __future__ import absolute_import

import pytest
from six.moves import builtins

try:
    # In python 3.3+ mock got included in the standard library...
    from unittest import mock
except ImportError:
    import mock

from pyleus.storm.component import Component


class ComponentTestCase(object):
    """Base class to inherit for testing pyleus components."""

    INSTANCE_CLS = Component

    @pytest.fixture(autouse=True)
    def instance_fixture(self):
        """Give access to a mock pyleus component through ``self.instance``."""
        self.mock_input_stream = mock.Mock()
        self.mock_output_stream = mock.Mock()
        self.instance = self.INSTANCE_CLS(
            input_stream=self.mock_input_stream,
            output_stream=self.mock_output_stream,
        )

assert builtins  # pyflakes
