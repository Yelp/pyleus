import pytest

from pyleus.testing import mock
from pyleus.storm.serializers.serializer import Serializer


class SerializerTestCase(object):

    INSTANCE_CLS = Serializer

    @pytest.fixture(autouse=True)
    def instance_fixture(self):
        self.mock_input_stream = mock.Mock()
        self.mock_output_stream = mock.Mock()
        self.instance = self.INSTANCE_CLS(
            input_stream=self.mock_input_stream,
            output_stream=self.mock_output_stream,
        )
