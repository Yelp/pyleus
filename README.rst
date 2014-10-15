Pyleus
======

Pyleus is a Python 2.6+ framework for developing and launching `Apache Storm`_ topologies.

Please visit our `wiki`_.

About
-----

Pyleus is a Python layer built on top of Apache Storm making possible to write, build and manage pure Python Storm topologies in a pythonic way.

With Pyleus you can:

* define a topology writing just a simple YAML file

* list your dependencies in a requirements.txt file (and forget about it)

* run faster thanks to Pyleus’ `MessagePack`_ based serializer

* pass options to your components directly from the YAML file

* leverage built-in support for Storm tick tuples

* use the kafka-spout Java implementation provided by Apache Storm just including it in the YAML file

Install
-------

From PyPI:

.. code-block:: shell

   $ pip install pyleus

**Note:**

You do **NOT** need to install pyleus on your Storm cluster. That’s cool, isn't it?

Try it out!
-----------

.. code-block:: shell

   $ git clone https://github.com/Yelp/pyleus.git
   $ pyleus build pyleus/examples/exclamation_topology/pyleus_topology.yaml
   $ pyleus local exclamation_topology.jar

Or, submit to a Storm cluster with:

.. code-block:: shell

   $ pyleus submit -n NIMBUS_IP exclamation_topology.jar

The ``examples`` folder contains several commented Pyleus topologies trying to cover all Pyleus features as much as possible.

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

     $ pyleus submit [-n NIMBUS_IP] /path/to/pyleus_topology.yaml

* List all topologies running on a Storm cluster:

  .. code-block:: shell

     $ pyleus list [-n NIMBUS_IP]

* Kill a topology running on a Storm cluster:

  .. code-block:: shell

     $ pyleus kill [-n NIMBUS_IP] TOPOLOGY_NAME

Try ``pyleus -h`` for a list of all the available commands or ``pyleus CMD -h`` for any command-specific help.

Write your first topology
-------------------------

Please refer to the `wiki`_ for a more detailed tutorial.

Organize your files
^^^^^^^^^^^^^^^^^^^

This is an example of the directory tree of a simple topology:

.. code-block::
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

   # This is a very meaningful paragraph
   # describing my_first_topology

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
               self.emit((This is a sentence."I am a stupid ", "spout",))


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
               sentence, _ = tup.values
               new_sentence = sentence + "bolt"
               self.emit((new_sentence,), anchors=[tup])

   if __name__ == '__main__':
       DummyBolt().run()

Run your topology
^^^^^^^^^^^^^^^^^

Run your topology on your local machine for debugging:

.. code-block:: shell

   pyleus build my_first_topology/pyleus_topology.yaml
   pyleus local my_first_topology.yaml -d

When you are done, hit ``C-C``.

Configuration File
^^^^^^^^^^^^^^^^^^

You can override Pyleus default configuration placing a `.pyleus.conf` configuration file in your home directory:

.. code-block:: none

   [storm]
   nimbus_ip: 10.11.12.13
   jvm_opts: -Djava.io.tmpdir=/home/myuser/tmp

   [build]
   pypi_index_url: http://pypi.ninjacorp.com/simple/

Reference
---------
*  `Apache Storm Documentation`_

License
-------

Pyleus is licensed under Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0


.. _Apache Storm: https://storm.incubator.apache.org/
.. _Apache Storm Documentation: https://storm.incubator.apache.org/documentation/Home.html
.. _MessagePack: http://msgpack.org/
.. _wiki: http://yelp.github.io/pyleus/
