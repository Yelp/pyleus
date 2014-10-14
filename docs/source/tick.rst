.. _tick:

Timing using tick tuples
========================

Tick tuples are system generated tuples that Storm can send to your bolt if you need to perform some actions at a fixed interval.

You can use the module level function :func:`pyleus.storm.is_tick` to tell whether a tuple is a tick tuple or not.

.. seealso:: You can read more about tick tuples `here`_.

Configure tick tuples interval for a component
----------------------------------------------

You can activate and configure tick tuples interval for a component using the ``tick_freq_secs`` option in the topology definition YAML file: 

.. code-block:: python

    - bolt:
        name: tick-bolt
        module: tick_topology.tick_bolt
        groupings:
            - shuffle_grouping: my-spout
        tick_freq_secs: 2.5

The specified value has to be ``float`` and it is intended as the interval in seconds between two tick tuples.

You can retrieve the frequency value from within a bolt using property ``self.conf.tick_tuple_freq``.

SimpleBolt tick tuple API
-------------------------

:class:`~pyleus.storm.bolt.SimpleBolt` offers a nicer API for handling tick tuples. Method :meth:`~pyleus.storm.bolt.SimpleBolt.process_tick` will be called instead of :meth:`~pyleus.storm.bolt.SimpleBolt.process_tuple` any time the bolt receives a tick tuple. In this way you can easily separate the code you want to execute for "real" tuple from the one you want to executed at a fixed interval.

.. seealso::

   You can find many examples in the `GitHub repo`_. 


.. _here: https://storm.apache.org/2012/08/02/storm080-released.html 
.. _Github repo: https://github.com/Yelp/pyleus/tree/master/examples
