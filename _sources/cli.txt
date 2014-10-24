.. _cli:

Command Line
============

* Build a topology:

  .. code-block:: none

     pyleus build /path/to/pyleus_topology.yaml [-o OUTPUT_JAR]

  This command will generate a topology jar file ready to be executed by Storm.

  The output jar will be named as the directory containing the YAML definition file passed as argument.
  Option ``--output`` allows to specify the output jar path.

  If a ``requirements.txt`` file is present in the same directory of the YAML topology definition file, all dependencies listed in the file will be included in the jar.

  .. seealso:: If you want to specify a different path for your requirements file, please see :ref:`yaml`. If you want to install some dependencies for all your topologies, see :ref:`configuration` instead.

* Run a topology locally:

  .. code-block:: none

     pyleus local /path/to/topology.jar [--debug]

  Hit ``C-C`` to stop local execution.

  The ``debug`` option will print evry tuple flowing through the topology.

* Submit a topology to a Storm cluster:

  .. code-block:: none

     pyleus submit [-n NIMBUS_HOST] [-p NIMBUS_PORT] /path/to/pyleus_topology.yaml

* List all topologies running on a Storm cluster:

  .. code-block:: none

     pyleus list [-n NIMBUS_HOST] [-p NIMBUS_PORT]

* Kill a topology running on a Storm cluster:

  .. code-block:: none

     pyleus kill [-n NIMBUS_HOST] [-p NIMBUS_PORT] TOPOLOGY_NAME [-w WAIT_TIME]

  Option ``--wait-time`` overrides the duration in seconds Storm waits between deactivation and shutdown. Storm's default is 30 seconds.

* You can specify a configuration file any time using option:

  .. code-block:: none

     pyleus -c path/to/config_file CMD

  .. seealso:: :ref:`configuration`


.. tip::

   Try ``pyleus -h`` for a list of all the available commands or ``pyleus CMD -h`` for any command-specific help.
