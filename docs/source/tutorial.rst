.. _tutorial:

Write your first topology
=========================

Organize your files
-------------------

This is an example of the directory tree of a simple topology:

.. code-block:: none

   my_first_topology
   |-- my_first_topology
   |   |-- __init__.py
   |   |-- dummy_bolt.py
   |   |-- dummy_spout.py
   |-- pyleus_topology.yaml
   |-- requirements.txt

When building your topology, the jar file generated from the ``pyleus build`` command will be named `my_first_topology.jar`, as the directory containing the YAML topology definition file.

File ``requirements.txt`` should list all the dependencies of the topology to be included in the jar. In this case, this file is empty.

  .. seealso:: If you want to specify a different path for your requirements file, please see :ref:`yaml`. If you want to install some dependencies for all your topologies, see :ref:`configuration` instead.

Define the topology layout
--------------------------

A simple ``pyleus_topology.yaml`` should look like the following:

.. code-block:: yaml

   # This is a very meaningful paragraph
   # describing my_first_topology

   name: my_first_topology

   workers: 2

   topology:

       - spout:
           name: my-first-spout
           module: my_first_topology.dummy_spout

       - bolt:
           name: my-first-bolt
           module: my_first_topology.dummy_bolt
           groupings:
               - shuffle_grouping: my-first-spout

This define a topology where a single bolt subscribe to the output stream of a single spout. As simple as it is.

.. note::

   Components names do NOT need to match modules names. This is because the same module may be reused more than once in the same topology, perhaps with different input streams or options.

.. tip::

   If you do not specify the number of workers for your topology, Storm will span just **one** worker. This is perfectly fine if you want to run your topology on your local machine, but you may like to change this value when running your topology on a real cluster. You can do that with the ``workers`` option as shown in the example above.

Write your first spout
----------------------

This is the code implementing ``dummy_spout.py``:

.. code-block:: python

   from pyleus.storm import Spout

   class DummySpout(Spout):

       OUTPUT_FIELDS = ['sentence', 'name']

       def next_tuple(self):
           self.emit(("This is a sentence.", "spout",))

   if __name__ == '__main__':
       DummySpout().run()

Every :class:`~pyleus.storm.spout.Spout` must inherit from :class:`~pyleus.storm.spout.Spout` and declare its :attr:`~pyleus.storm.component.Component.OUTPUT_FIELDS` as a ``tuple``, a ``list`` or a ``namedtuple``. The same goes for ``emit`` **first argument**.

Spouts also must define the method :meth:`~pyleus.storm.spout.Spout.next_tuple`, that will be called within the component main loop in order to generate a stream of new tuples.

.. note:: Forgetting to call the :meth:`~pyleus.storm.component.Component.run` method will prevent the topology from running.

.. seealso:: If you want to enable tuple tracking and leverage Storm reliability features, please read :ref:`reliability`.

.. seealso:: For complete API documentation, see :ref:`spout`.

Write your first bolt
---------------------

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

Every :class:`~pyleus.storm.bolt.Bolt` must inherit from :class:`~pyleus.storm.bolt.Bolt` or :class:`~pyleus.storm.bolt.SimpleBolt`, which is a bolt automatically acking/failing tuples and offering a nicer API to leverage tick tuples. The ``process_tuple`` method will be called whenever a new tuple reaches the bolt.

.. note::

   Please note that :class:`~pyleus.storm.bolt.SimpleBolt` will **NOT** automatically anchor your tuples. See :ref:`reliability` for more info on anchoring.

.. note::

   Even if you want to define only one output field, please declare it as an element either of a ``list`` or of a ``tuple``, as showed in the above example. Using just a ``string`` is not allowed.

.. seealso:: For complete API documentation, see :ref:`bolt`.

.. warning::

   Do **NOT** print. I'm gonna say it again: Do. Not. Print. Or, at least, do not print until you want to crash your topology. The mechanism Storm uses to communicate with Python is based on stdin/stdout communication, so you are not allowed to use them. Use logging instead (see :ref:`logging`).

Run your topology
-----------------

Run your topology on your local machine for debugging:

.. code-block:: none

   pyleus build my_first_topology/pyleus_topology.yaml
   pyleus local my_first_topology.jar -d

The ``-debug`` option will print all tuples flowing through the topology.

When you are done, hit ``C-C``.
