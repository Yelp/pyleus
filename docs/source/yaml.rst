.. _yaml:

Topology definition YAML syntax
===============================

Options whose default is not specified on this page use the same `default`_ as the corresponding `Apache Storm configuration option`_.

Topology level options
----------------------

* **name**\(``str``\)[mandatory]

  Name assigned to your topology.

* **topology**\(``seq``\)[mandatory]

  Sequence containing all components of the topology, where each component is a ``map``. Allowed components: ``spout``, ``bolt``.

* **workers**\(``int``\)

  Number of workers to be spawned.

* **ackers**\(``int``\)

  Number of executors for ackers to be spawned. Corresponds to Storm ``TOPOLOGY_ACKER_EXECUTORS``.

* **max_spout_pending**\(``int``\)

  Maximum number of tuples that can be pending on a spout task at any given time.

* **message_timeout_secs**\(``int``\)

  Maximum amount of time given to the topology to fully process a message emitted by a spout.

* **max_shellbot_pending**\(``int``\)

  Maximum pending tuples in one ShellBolt. Default: 1

* **topology_debug**\(``boolean``\)

  Enable running topology in DEBUG mode. Corresponds to Storm ``TOPOLOGY_DEBUG``.

* **sleep_spout_wait_strategy_time_ms**\(``int``\)

  The amount of milliseconds the SleepEmptyEmitStrategy should sleep for.

* **worker_childopts_xmx**\(``str``\)

  Topology-specific options for the worker child process, it is initially used to set heap size for worker. Corresponds to Storm ``TOPOLOGY_WORKER_CHILDOPTS``

* **executor_receive_buffer_size**\(``int``\)

  The size of the Disruptor receive queue for each executor. Must be a power of 2. Corresponds to Storm ``TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE``

* **executor_send_buffer_size**\(``int``\)

  The size of the Disruptor send queue for each executor. Must be a power of 2. Corresponds to Storm ``TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE``

* **transfer_buffer_size**\(``int``\)

  The size of the Disruptor transfer queue for each worker. Corresponds to Storm ``TOPOLOGY_TRANSFER_BUFFER_SIZE``

* **logging_conf**\(``str``\)

  Path to logging configuration file. Default: ``<yaml_file_dir>/pyleus_logging.conf``. Specify ``none`` if a file corresponds to the default path, but you want to ignore it.

* **requirements_filename**\(``str``\)

  Path to the file listing topology requirements. Default: ``<yaml_file_dir>/requirements.txt``. Specify ``none`` if a file corresponds to the default path, but you want to ignore it.

* **python_interpreter**\(``str``\)

  The Python interpreter to use to create the topology virtualenv (exposes ``virtualenv`` ``--python`` option). Default: the interpreter that virtualenv was installed with (``/usr/bin/python``).

* **serializer**\(``str``\)

  Serializer used by Pyleus for Stom multilang messages. Allowed: ``msgpack``, ``json``. Default: ``msgpack``.

  .. note::

     If you want to use JSON as encoding format Storm multilang messages, you can switch between Python standard library `json`_ module and `simplejson`_ module specifying ``simplejson`` in the **requirements** for your topology.

  .. tip::

     If you are on Python 2.6, we strongly recommend `simplejson`_ over `json`_ for better performance.

Component level options
-----------------------

These options belong to the block associated either with a ``spout`` or a ``bolt`` component.

* **name**\(``str``\)[mandatory]

  Name assigned to the component.

* **module**\(``str``\)[mandatory]

  Python module containing the code for that component (e.g. ``my_topology.my_spout``). Every valid module should contain a class inheriting either from :class:`~pyleus.storm.spout.Spout` or from :class:`~pyleus.storm.bolt.Bolt`. The module should also call the component :meth:`~pyleus.storm.component.Component.run` method when ``__name__ == '__main__'``.

* **type**\(``str``\)

  Ad-hoc option to be used instead of ``module`` to specify the Storm Kafka Spout component. Allowed: ``kafka``, ``python``.

  .. note::

     Only inside a ``spout`` block, you can specify ``type: kafka`` **instead** of ``module``.

  .. seealso::

     Refer to this `example`_ for all kafka related options.

* **parallelism_hint**\(``int``\)

  Initial number of executors per component.

* **tasks**\(``int``\)

  Number of tasks per component.

* **tick_freq_secs**\(``float``\)[only for ``bolt``]

  Interval in seconds between two consecutive tick tuples.

* **options**\(``map``\)

  Block containing options to be passed to the component.

  .. seealso::

     :ref:`options`.

* **groupings**\(``seq``\)[mandatory only for ``bolt``]

  Sequence of groupings specifying the input streams for the component.

  .. seealso::

     For grouping specific syntax, please refer to :ref:`groupings`.

.. _json: https://docs.python.org/2/library/json.html
.. _simplejson: http://simplejson.readthedocs.org/en/latest/
.. _default: https://github.com/apache/storm/blob/master/conf/defaults.yaml
.. _Apache Storm configuration option: https://storm.incubator.apache.org/apidocs/backtype/storm/Config.html
.. _example: https://github.com/Yelp/pyleus/tree/master/examples/kafka_spout
