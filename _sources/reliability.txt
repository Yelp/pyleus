.. _reliability:

Guaranteeing message processing
===============================

You can find more info about Storm reliability features in the `Apache Storm Documentation`_.

Track tuples in your Spout
--------------------------
In order to make Storm track your tuples, you need to pass a *tuple identifier* as ``tup_id`` when emitting each tuple:

.. code-block:: python

   self.emit((value,), tup_id=a_unique_id)

In addition, you should also implement methods :meth:`~pyleus.storm.spout.Spout.ack` and :meth:`~pyleus.storm.spout.Spout.fail`, in order to ack back to your input source whether your tuple has been fully processed or not and to re-emit it, if that is the case.

.. seealso:: For complete API documentation, see :ref:`spout`.

Anchor tuples in your Bolt
--------------------------
At the :class:`~pyleus.storm.bolt.Bolt` level, if you want to extend tracking to newly emitted tuples, you should specify as ``anchors`` a ``list`` containing all the parent tuples for the tuple you are emitting: 

.. code-block:: python

   self.emit((word,), anchors=[parent_tuple])

.. warning:: 

   All tuples need to be acked or failed, independently whether you are using Storm reliability features or not.
   If you are directly using :class:`~.Bolt` instead of :class:`~.SimpleBolt`, you must call this method or your topology will eventually run out of memory or hang.

.. seealso:: For complete API documentation, see :ref:`bolt`.

Tune your topology
------------------
If you are using Storm reliability features, you also need to run **at least one** Storm acker, otherwise your topology will hang. You can specify the number of ackers for your topology in the topology definition YAML file using the ``ackers`` option.

We strongly encourage you to tune the maximum number of tuples that can be pending on a spout task at any given time, too. You can do that using the topology level option ``max_spout_pending``. 

Finally, you may also want to specify the maximum amount of time given to the topology to fully process a message emitted by a spout before failing it. This can be done with option ``message_timeout_secs``. 

.. code-block:: yaml

   name: reliable_topology

   ackers: 3
   max_spout_pending: 100
   message_timeout_secs: 300

.. seealso::

   For a detailed explanation of those settings and for their default values, please take a look at `Apache Storm Config`_, `Apache Storm FAQ`_ and `Apache Storm defaults.yaml`_. 

.. _Apache Storm Documentation: https://storm.apache.org/documentation/Guaranteeing-message-processing.html  
.. _Apache Storm FAQ: https://storm.apache.org/documentation/FAQ.html
.. _Apache Storm Config: https://storm.incubator.apache.org/apidocs/backtype/storm/Config.html
.. _Apache Storm defaults.yaml: https://github.com/apache/storm/blob/master/conf/defaults.yaml
