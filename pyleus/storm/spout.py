from __future__ import absolute_import

import logging
import traceback

from pyleus.storm import StormWentAwayError
from pyleus.storm.component import Component

log = logging.getLogger(__name__)


class Spout(Component):

    COMPONENT_TYPE = "spout"

    def next_tuple(self):
        """Implement in subclass"""
        pass

    def ack(self, tup_id):
        """Implement in subclass"""
        pass

    def fail(self, tup_id):
        """Implement in subclass"""
        pass

    def _handle_command(self, msg):
        command = msg['command']

        if command == 'next':
            self.next_tuple()
        elif command == 'ack':
            self.ack(msg['id'])
        elif command == 'fail':
            self.fail(msg['id'])

    def _sync(self):
        self.send_command('sync')

    def run_component(self):
        try:
            while True:
                msg = self.read_command()
                self._handle_command(msg)
                self._sync()
        except StormWentAwayError as e:
            log.warning("Disconnected from Storm. Exiting.")
        except Exception as e:
            log.exception("Exception in Spout.run")
            self.error(traceback.format_exc(e))

    def emit(self, values, stream=None, tup_id=None, direct_task=None):
        """Build and send an output tuple command dict; return the tasks to
        which the tuple was sent by Storm.

        tup_id should be JSON-serializable.
        """
        assert isinstance(values, list) or isinstance(values, tuple)

        command_dict = {
            'tuple': tuple(values),
        }

        if stream is not None:
            command_dict['stream'] = stream

        if tup_id is not None:
            command_dict['id'] = tup_id

        if direct_task is not None:
            command_dict['task'] = direct_task

        self.send_command('emit', command_dict)
        return self.read_taskid()
