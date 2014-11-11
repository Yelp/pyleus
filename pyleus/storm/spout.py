"""Module containing the implementation of the Spout component."""
from __future__ import absolute_import

import logging

from pyleus.storm import StormWentAwayError
from pyleus.storm.component import Component

log = logging.getLogger(__name__)


class Spout(Component):
    """Spout component class. Inherit from
    :class:`~pyleus.storm.component.Component`.
    """

    COMPONENT_TYPE = "spout"

    def next_tuple(self):
        """Emit the next tuple into the topology.

        .. note:: Implement in subclass.
        """
        pass

    def ack(self, tup_id):
        """Ack a tuple to the source.

        :param tup_id: tuple identifier
        :type tup_id: ``str`` or ``long``

        .. note:: Implement in subclass. Default behaviour is ``pass``.
        """
        pass

    def fail(self, tup_id):
        """Fail a tuple to the source.

        :param tup_id: tuple identifier
        :type tup_id: ``str`` or ``long``

        .. note:: Implement in subclass. Default behaviour is ``pass``.
        """
        pass

    def _handle_command(self, msg):
        """Switch on the type of command."""
        command = msg['command']

        if command == 'next':
            self.next_tuple()
        elif command == 'ack':
            self.ack(msg['id'])
        elif command == 'fail':
            self.fail(msg['id'])

    def _sync(self):
        """Send a sync message."""
        self.send_command('sync')

    def run_component(self):
        """Spout main loop."""
        try:
            while True:
                msg = self.read_command()
                self._handle_command(msg)
                self._sync()
        except StormWentAwayError:
            log.warning("Disconnected from Storm. Exiting.")

    def emit(
            self, values,
            stream=None, tup_id=None,
            direct_task=None, need_task_ids=True):
        """Build and send an output tuple command dict and return the ids of
        the tasks to which the tuple was sent by Storm.

        :param values: pyleus tuple values to be emitted
        :type values: ``tuple`` or ``list``
        :param stream:
         output stream the message is going to belong to, default ``DEFAULT``
        :type stream: ``str``
        :param tup_id:
         identifier that will be used by Storm for tracking the tuple for
         reliability purpose. It will be passed as argument to both
         :meth:`~.ack` and :meth:`~.fail` when the tuple terminates its
         lifecycle. Default ``None``
        :type tup_id: ``str`` or ``long``
        :param direct_task: task message will be sent to, default None
        :type direct_task: ``int``
        :param need_task_ids:
         whether emit should return the ids of the task the message has been
         sent to, default ``True``
        :type need_task_ids: ``bool``

        .. note:: ``tup_id`` should be JSON-serializable.

        .. note::
           Omitting ``tup_id`` will disable reliability tracking for that
           tuple. If you provide a value for ``tup_id``, then you also need to
           run at least one Storm **acker** (see :ref:`reliability`),
           otherwise your topology will hang.

        .. tip::
           Setting ``need_task_ids`` to ``False`` really helps in achieving
           better performances. You should always do that if your application
           does not leverage task ids.

        .. danger::
           ``direct_task`` is not yet supported.

        """
        assert isinstance(values, list) or isinstance(values, tuple)

        command_dict = {
            # Different versions of simplejson serialize namedtuples differently.
            # Cast to tuple in order to have consistent
            # behavior between msgpack, json and simplejson.
            'tuple': tuple(values),
        }

        if stream is not None:
            command_dict['stream'] = stream

        if tup_id is not None:
            command_dict['id'] = tup_id

        if direct_task is not None:
            command_dict['task'] = direct_task

        # By default, Storm sends back to the component the task ids of the
        # tasks receiving the tuple. If need_task_ids is set to False, Storm
        # won't send the task ids for that message
        if not need_task_ids:
            command_dict['need_task_ids'] = False

        self.send_command('emit', command_dict)

        if need_task_ids:
            return self.read_taskid()
