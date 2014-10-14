.. _parallelism:

Tune topology parallelism
=========================

Assuming that you are familiar with Storm parallelism concepts, pyleus replicates the same controls offered by Storm.

.. seealso::  For a deep dive into Storm parallelism, refer to `Apache Storm Documentation`_. 

* You can specify the number of workers **per topology** using the ``workers`` tag in the topology YAML definition file. 

* You can set the **initial** number of executors **per component** with the ``parallelism_hint`` option.

* You can set the number of tasks **per component** (**NOT** per executors) via ``tasks``. 

.. code-block:: yaml

    name: parallel_topology

    workers: 3 

    bolt:
        name: my-parallel-bolt
        module: parallel_topology.a_bolt 
        groupings:
            - shuffle_grouping: a-spout
        parallelism_hint: 3
        tasks: 6 

.. warning::

   Pyleus is **NOT** thread-safe when it comes to emitting tuples. If you are using threads, please take this into account.

.. tip::

   Storm is rather unhappy if you try to use threads in order to parallelize your topology components. Use the ``parallelism_hint`` tag instead.

.. _Apache Storm Documentation: https://storm.apache.org/documentation/Understanding-the-parallelism-of-a-Storm-topology.html 
