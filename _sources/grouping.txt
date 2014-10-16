.. _groupings:

Output streams and groupings
=============================

Single output streams
---------------------

You can define a component with a single output stream just assigning a ``list`` or a ``tuple`` to the class attribute :attr:`~pyleus.storm.component.Component.OUTPUT_FIELDS`.

.. code-block:: python

   class CountingBolt(Bolt):

       OUTPUT_FIELDS = ['word', 'counter']

Pyleus also accepts ``namedtuple`` for output fields schema declaration:

.. code-block:: python

   Record = namedtuple('Record', 'year month day value')

   class SampleBolt(Bolt):

      OUTPUT_FIELDS = Record 

This allows you to import your schema in the downstream component and to build code that is more resilient to schema changes (at the price of building nametuples from incoming tuple values). You can find some examples of this technique in the `examples`_ folder.

When you define a single output stream, all emitted tuples will go to the ``default`` stream and you are then allowed to use the short definition syntax for groupings: 

.. code-block:: yaml

   - bolt:
       name: my-first-bolt
       module: my_topology.my_first_bolt
       groupings:
           - shuffle_grouping: my-spout

   - bolt:
       name: my-second-bolt
       module: my_topology.my_second_bolt
       groupings:
           - fields_grouping:
               component: my-first-bolt
               fields:
                   - year
                   - month

.. note::

   Please note that, while spouts **must** define at least an output stream, bolts do not have to. 

Multiple output streams
-----------------------

Storm allows components to define an arbitrary numbers of output streams. For doing that in Pyleus, you need to define your output streams in a ``dict`` as the following:

.. code-block:: python 

   class MultipleBolt(Bolt):

       OUTPUT_FIELDS = {
           "stream-id": ["id", "value"],
           "stream-date": ["year", "month", "day", "value"],
       }
 
As a consequence you need to use the complete definition syntax for groupings in the topology YAML file:

.. code-block:: yaml

   - bolt:
       name: my-first-bolt
       module: my_topology.my_first_bolt
       groupings:
           - shuffle_grouping:
               component: my-first-spout
               stream: stream-A

   - bolt:
       name: my-second-bolt
       module: my_topology.my_second_bolt
       groupings:
           - fields_grouping:
               component: my-first-bolt
               stream: stream-date
               fields:
                   - year
                   - month

.. seealso::

   See `GitHub`_ for an example topology declaring multiple output streams.

Available stream groupings
--------------------------

* Shuffle grouping:

  .. code-block:: yaml
  
     - shuffle_grouping:
         component: a-component
         stream: a-stream

* Local or shuffle grouping:

  .. code-block:: yaml
  
     - local_or_shuffle_grouping:
         component: a-component
         stream: a-stream

* Global grouping:

  .. code-block:: yaml
  
     - global_grouping:
         component: a-component
         stream: a-stream

* All grouping:

  .. code-block:: yaml
  
     - all_grouping:
         component: a-component
         stream: a-stream

* None grouping:

  .. code-block:: yaml
  
     - none_grouping:
         component: a-component
         stream: a-stream

* Fields grouping:

  .. code-block:: yaml

     - fields_grouping:
         component: a-component 
         stream: a-stream
         fields:
             - a-field
             - another-field

.. danger:: 

   Storm **direct grouping** is not yet supported.

.. seealso::

   For a complete reference of Storm groupings see `Apache Storm Documentation`_. 

.. _examples: https://github.com/Yelp/pyleus/tree/master/examples
.. _GitHub: https://github.com/Yelp/pyleus/tree/master/examples/micro
.. _Apache Storm Documentation: https://storm.apache.org/documentation/Concepts.html 
