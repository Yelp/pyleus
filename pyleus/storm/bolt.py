from __future__ import absolute_import

import logging
import traceback

from pyleus.storm import is_tick, StormWentAwayError
from pyleus.storm.component import Component

log = logging.getLogger(__name__)


class Bolt(Component):

    COMPONENT_TYPE = "bolt"

    def process_tuple(self, tup):
        """Implement in subclass"""
        pass

    def _process_tuple(self, tup):
        """Implement in bolt middleware

        Bolt middleware classes such as SimpleBolt should override this to
        inject functionality around tuple processing without changing the
        API for downstream bolt implementations.
        """
        return self.process_tuple(tup)

    def run_component(self):
        try:
            while True:
                tup = self.read_tuple()
                self._process_tuple(tup)
        except StormWentAwayError as e:
            log.warning("Disconnected from Storm. Exiting.")
        except Exception as e:
            log.exception("Exception in Bolt.run")
            self.error(traceback.format_exc(e))

    def ack(self, tup):
        self.send_command('ack', {
            'id': tup.id,
        })

    def fail(self, tup):
        self.send_command('fail', {
            'id': tup.id,
        })

    def emit(self, values, stream=None, anchors=None, direct_task=None):
        """Build and send an output tuple command dict; return the tasks to
        which the tuple was sent by Storm.
        """
        assert isinstance(values, list) or isinstance(values, tuple)

        if anchors is None:
            anchors = []

        command_dict = {
            'anchors': [anchor.id for anchor in anchors],
            'tuple': tuple(values),
        }

        if stream is not None:
            command_dict['stream'] = stream

        if direct_task is not None:
            command_dict['task'] = direct_task

        self.send_command('emit', command_dict)
        return self.read_taskid()


class SimpleBolt(Bolt):
    """A Bolt that automatically acks or fails tuples.

    Implement process_tick() in a subclass to handle tick tuples with a nicer
    API.
    """

    def process_tick(self):
        """Implement in subclass"""
        pass

    def _process_tuple(self, tup):
        try:
            if is_tick(tup):
                self.process_tick()
            else:
                self.process_tuple(tup)
        except:
            self.fail(tup)
            raise
        else:
            self.ack(tup)
