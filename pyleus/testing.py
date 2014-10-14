"""Helper functions and classes for testing."""
from __future__ import absolute_import

import mock
import pytest

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
