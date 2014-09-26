from cStringIO import StringIO
import mock
import os

import msgpack

from pyleus.storm.serializers.msgpack_serializer import MsgpackSerializer
from testing.serializer import SerializerTestCase


class TestMsgpackSerializer(SerializerTestCase):

    INSTANCE_CLS = MsgpackSerializer

    def test_read_msg_dict(self):
        msg_dict = {
            'hello': "world",
        }

        encoded_msg = msgpack.packb(msg_dict)

        with mock.patch.object(
                os, 'read', return_value=encoded_msg):

            assert self.instance.read_msg() == msg_dict

    def test_read_msg_list(self):
        msg_list = [3, 4, 5]

        encoded_msg = msgpack.packb(msg_list)

        with mock.patch.object(
                os, 'read', return_value=encoded_msg):

            assert self.instance.read_msg() == msg_list

    def test_send_msg(self):
        msg_dict = {
            'hello': "world",
        }

        expected_output = msgpack.packb(msg_dict)

        with mock.patch.object(
                self.instance, '_output_stream', StringIO()) as sio:
            self.instance.send_msg(msg_dict)

        assert sio.getvalue() == expected_output
