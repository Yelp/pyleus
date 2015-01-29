"""Module containing the implementation of the Bolt component and a subclassed
SimpleBolt component which takes care of acking/failing tuples and exposing a
nicer API for handling tick tuples.
"""
from __future__ import absolute_import

import logging

from pyleus.storm import is_tick, is_heartbeat, StormWentAwayError
from pyleus.storm.component import Component

log = logging.getLogger(__name__)


class Bolt(Component):
    """Bolt component class. Inherit from
    :class:`~pyleus.storm.component.Component`.
    """

    COMPONENT_TYPE = "bolt"

    def process_tuple(self, tup):
        """Process the incoming tuple.

        :param tup: pyleus tuple representing the message to be processed
        :type tup: :class:`~pyleus.storm.StormTuple`

        .. note:: Implement in subclass.
        """
        pass

    def _process_tuple(self, tup):
        """Bolt middleware classes such as SimpleBolt should override this to
        inject functionality around tuple processing without changing the API
        for downstream bolt implementations.

        .. note: Implement in Bolt middleware subclass.
        """
        if is_heartbeat(tup):
            self.sync()
        else:
            return self.process_tuple(tup)

    def run_component(self):
        """Bolt main loop."""
        try:
            while True:
                tup = self.read_tuple()
                self._process_tuple(tup)
        except StormWentAwayError:
            log.warning("Disconnected from Storm. Exiting.")

    def ack(self, tup):
        """Ack a tuple.

        :param tup: tuple to ack
        :type tup: :class:`~pyleus.storm.StormTuple`

        .. note::
           All tuples need to be acked or failed, independently whether
           you are using Storm reliability features or not. If you are directly
           using :class:`~.Bolt` instead of :class:`~.SimpleBolt`, you must
           call this method or your topology will eventually run out of memory
           or hang.
        """
        self.send_command('ack', {
            'id': tup.id,
        })

    def fail(self, tup):
        """Fail a tuple.

        :param tup: tuple to fail
        :type tup: :class:`~pyleus.storm.StormTuple`

        .. note::
           All tuples need to be acked or failed, independently whether
           you are using Storm reliability features or not. If you are directly
           using :class:`~.Bolt` instead of :class:`~.SimpleBolt`, you must
           call this method or your topology will eventually run out of memory
           or hang.
        """
        self.send_command('fail', {
            'id': tup.id,
        })

    def sync(self):
        """Respond to heartbeat.
        """
        self.send_command('sync')

    def emit(
            self, values,
            stream=None, anchors=None,
            direct_task=None, need_task_ids=True):
        """Build and send an output tuple command dict and return the ids of
        the tasks to which the tuple was sent by Storm.

        :param values: pyleus tuple values to be emitted
        :type values: ``tuple`` or ``list``
        :param stream:
         output stream the message is going to belong to, default ``DEFAULT``
        :type stream: ``str``
        :param anchors:
         list of pyleus tuples the message should be anchored to, default
         ``None``
        :type anchors: ``list`` of pyleus tuples
        :param direct_task: task message will be sent to, default None
        :type direct_task: ``int``
        :param need_task_ids:
         whether emit should return the ids of the task the message has been
         sent to, default ``True``
        :type need_task_ids: ``bool``

        .. tip::
           Setting ``need_task_ids`` to ``False`` really helps in achieving
           better performances. You should always do that if your application
           does not leverage task ids.

        .. danger::
           ``direct_task`` is not yet supported.
        """
        assert isinstance(values, list) or isinstance(values, tuple)

        if anchors is None:
            anchors = []

        command_dict = {
            'anchors': [anchor.id for anchor in anchors],
            # Different versions of simplejson serialize namedtuples differently.
            # Cast to tuple in order to have consistent
            # behavior between msgpack, json and simplejson.
            'tuple': tuple(values),
        }

        if stream is not None:
            command_dict['stream'] = stream

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


class SimpleBolt(Bolt):
    """A Bolt that automatically acks/fails tuples.

    Implement process_tick() in a subclass to handle tick tuples with a nicer
    API.
    """

    def process_tick(self):
        """Code to be executed when a tick tuple reaches the component.

        .. note:: Implement in subclass."""
        pass

    def _process_tuple(self, tup):
        """SimpleBolt middleware level tuple processing."""
        if is_heartbeat(tup):
            self.sync()
        else:
            if is_tick(tup):
                self.process_tick()
            else:
                self.process_tuple(tup)

            self.ack(tup)
