from __future__ import absolute_import

import mock
import pytest

from pyleus.storm.component import Component


class ComponentTestCase(object):

    INSTANCE_CLS = Component

    @pytest.fixture(autouse=True)
    def instance_fixture(self):
        self.mock_input_stream = mock.Mock()
        self.mock_output_stream = mock.Mock()
        self.instance = self.INSTANCE_CLS(
            input_stream=self.mock_input_stream,
            output_stream=self.mock_output_stream,
        )


class StormComponentTestCase(ComponentTestCase):
    """This class was renamed to ComponentTestCase.

    Remove in next major release.
    """

    def __init__(self, *args, **kwargs):
        import warnings
        warnings.warn("This class has been renamed to ComponentTestCase",
                      DeprecationWarning)
        super(StormComponentTestCase, self).__init__(*args, **kwargs)
