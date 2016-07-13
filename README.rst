Pyleus
======

Pyleus is a Python 2.6+ framework for developing and launching `Apache Storm`_ topologies.

Please visit our `documentation`_.

===============  ================
    master           develop
===============  ================
|master-status|  |develop-status|
===============  ================

.. |master-status| image:: https://travis-ci.org/YelpArchive/pyleus.svg?branch=master
    :target: https://travis-ci.org/YelpArchive/pyleus

.. |develop-status| image:: https://travis-ci.org/YelpArchive/pyleus.svg?branch=develop
    :target: https://travis-ci.org/YelpArchive/pyleus

About
-----

Pyleus is a framework for building Apache Storm topologies in idiomatic Python.

With Pyleus you can:

* define a topology with a simple YAML file

* have dependency management with a ``requirements.txt`` file

* run faster thanks to Pyleus’ `MessagePack`_ based serializer

* pass options to your components directly from the YAML file

* use the Kafka spout built into Storm with only a YAML change

Install
-------

From PyPI:

.. code-block:: shell

   $ pip install pyleus

**Note:**

You do **NOT** need to install pyleus on your Storm cluster. That’s cool, isn't it?

However, if you are going to use ``system_site_packages: true`` in your config file, you should be aware that the environment of your Storm nodes needs to match the one on the machine used for building the topology. This means you actually **have to install** pyleus on your Storm cluster in this case.

Try it out!
-----------

.. code-block:: shell

   $ git clone https://github.com/Yelp/pyleus.git
   $ pyleus build pyleus/examples/exclamation_topology/pyleus_topology.yaml
   $ pyleus local exclamation_topology.jar

Or, submit to a Storm cluster with:

.. code-block:: shell

   $ pyleus submit -n NIMBUS_HOST exclamation_topology.jar

The `examples`_ directory contains several annotated Pyleus topologies that try to cover as many Pyleus features as possible.

Pyleus command line interface
-----------------------------

* Build a topology:

  .. code-block:: shell

     $ pyleus build /path/to/pyleus_topology.yaml

* Run a topology locally:

  .. code-block:: shell

     $ pyleus local /path/to/topology.jar

* Submit a topology to a Storm cluster:

  .. code-block:: shell

     $ pyleus submit [-n NIMBUS_HOST] /path/to/topology.jar

* List all topologies running on a Storm cluster:

  .. code-block:: shell

     $ pyleus list [-n NIMBUS_HOST]

* Kill a topology running on a Storm cluster:

  .. code-block:: shell

     $ pyleus kill [-n NIMBUS_HOST] TOPOLOGY_NAME

Try ``pyleus -h`` for a list of all the available commands or ``pyleus CMD -h`` for any command-specific help.

Write your first topology
-------------------------

Please refer to the `documentation`_ for a more detailed tutorial.

Organize your files
^^^^^^^^^^^^^^^^^^^

This is an example of the directory tree of a simple topology:

.. code-block:: none

   my_first_topology/
   |-- my_first_topology/
   |   |-- __init__.py
   |   |-- dummy_bolt.py
   |   |-- dummy_spout.py
   |-- pyleus_topology.yaml
   |-- requirements.txt

Define the topology layout
^^^^^^^^^^^^^^^^^^^^^^^^^^

A simple ``pyleus_topology.yaml`` should look like the following:

.. code-block:: yaml

   name: my_first_topology

   topology:

       - spout:
           name: my-first-spout
           module: my_first_topology.dummy_spout
    
       - bolt:
           name: my-first-bolt
           module: my_first_topology.dummy_bolt
           groupings:
               - shuffle_grouping: my-first-spout

This defines a topology where a single bolt subscribes to the output stream of a single spout. As simple as it is.

Write your first spout
^^^^^^^^^^^^^^^^^^^^^^

This is the code implementing ``dummy_spout.py``:

.. code-block:: python

   from pyleus.storm import Spout


   class DummySpout(Spout):

       OUTPUT_FIELDS = ['sentence', 'name']

       def next_tuple(self):
           self.emit(("This is a sentence.", "spout",))


   if __name__ == '__main__':
       DummySpout().run()

Write your first bolt
^^^^^^^^^^^^^^^^^^^^^

Let's now look at ``dummy_bolt.py``:

.. code-block:: python

   from pyleus.storm import SimpleBolt


   class DummyBolt(SimpleBolt):

       OUTPUT_FIELDS = ['sentence']

       def process_tuple(self, tup):
           sentence, name = tup.values
           new_sentence = "{0} says, \"{1}\"".format(name, sentence)
           self.emit((new_sentence,), anchors=[tup])


   if __name__ == '__main__':
       DummyBolt().run()

Run your topology
^^^^^^^^^^^^^^^^^

Run the topology on your local machine for debugging:

.. code-block:: shell

   pyleus build my_first_topology/pyleus_topology.yaml
   pyleus local --debug my_first_topology.jar

When you are done, hit ``C-C``.

Configuration File
^^^^^^^^^^^^^^^^^^

You can set default values for many configuration options by placing a ``.pyleus.conf`` file in your home directory:

.. code-block:: none

   [storm]
   nimbus_host: 10.11.12.13
   jvm_opts: -Djava.io.tmpdir=/home/myuser/tmp

   [build]
   pypi_index_url: http://pypi.ninjacorp.com/simple/

Reference
---------
*  `Apache Storm Documentation`_

License
-------

Pyleus is licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0


.. _Apache Storm: https://storm.apache.org/
.. _Apache Storm Documentation: https://storm.apache.org/documentation/Home.html
.. _MessagePack: http://msgpack.org/
.. _documentation: http://pyleus.org/
.. _examples: https://github.com/Yelp/pyleus/tree/master/examples
