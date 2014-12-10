.. _options:

Component runtime customization
===============================

Pass options to components
--------------------------

It is possible to pass options to pyleus components through the topology definition YAML file.

This is how to define options in the YAML:

.. code-block:: yaml

   - spout:
           name: my_spout_with_options
           module: my_topology.spout_with_options
           options:
               word: bird

While this is how to declare and refer to them in components:

.. code-block:: python

   class SpoutWithOptions(Spout):

       OUTPUT_FIELDS = ['line']
       OPTIONS = ['word']

    def next_tuple(self):
        self.emit(("I am a stupid ", self.options['word'],))

You can access options at any time through the ``self.options`` dictionary.

.. note::

   You do not need to specify in the YAML all options declared in the component, but accessing not specified options will raise an error.

   Conversely, passing options which have not been defined in the component through the YAML file will prevent your topology from building. You can think to components as functions where you define optional arguments (options) and to the YAML file as the code calling them.

Access component configuration and context
------------------------------------------

It is worth noting that you have access to a copy of Storm configuration and context for each component. You can access them at any time through ``self.conf`` and ``self.context`` dictionaries like in the following example:

.. code-block:: python

   topology_name = self.conf['storm.id']
   taskid = self.context['taskid']

While the content of ``self.conf`` is rather easy to imagine, ``self.context`` contains information like the following:

.. code-block:: python

   {'task->component': {1: '__acker', 2: 'my-spout', 3: 'my-bolt', 4: 'another-bolt'}, 'taskid': 3}

Setup component at startup
--------------------------

Sometimes it could be useful to perform some setup actions *before* your component starts receiving tuples, but *after* Storm configuration and context have been loaded and options have been parsed.

You can implement the :meth:`~pyleus.storm.component.Component.initialize` method, like in the example below, to achieve the same behavior:

.. code-block:: python

   class FileSpout(self):

       OPTIONS = ['filename']

       def initialize(self):
           with open(self.options['filename'], 'r') as f:
               self.lines = f.readlines()

.. seealso::

   You can find several examples on `GitHub`.

.. _examples: https://github.com/Yelp/pyleus/tree/master/examples
