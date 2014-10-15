.. _logging:

Logging
=======

.. warning::

   Do **NOT** print. I'm gonna say it again: Do. Not. Print. Or, at least, do not print until you want to crash your topology. The mechanism Storm uses to communicate with Python is based on stdin/stdout communication, so you are not allowed to use them.

You can use Python logging module for getting useuful information out of your running topology. It is extremely useful while debugging your application, since pyleus can automatically log any exception occured in the log you setup.

You can configure logging separately for each component in your topology, as many of the `examples`_ in the pyleus repo do. However, you can always specify a topology level configuration file as showed in the next section.

Topology level configuration
----------------------------

Pyleus will look for a file named ``pyleus_logging.conf`` in the directory where your topology definition YAML file is  and use it if present. You can specify a different file with the YAML ``logging_config`` option, or set it to ``null`` if ``pyleus_logging.conf`` is present but you do not want pyleus to use it.

.. code-block:: yaml

   name: logging_topology

   logging_config: my_logging.conf

You can find an example topology using logging on `GitHub`_.

.. _GitHub: https://github.com/Yelp/pyleus/tree/master/examples/logging
.. _examples: https://github.com/Yelp/pyleus/tree/master/examples
