Pyleus v\ |version|
===================

Pyleus is a Python 2.6+ layer built on top of `Apache Storm`_ making possible to write, build and manage pure Python Storm topologies in a pythonic way.

* Pyleus is available on pypi: https://pypi.python.org/pypi/pyleus
* The source is hosted on github: https://github.com/Yelp/pyleus

.. warning::

   Pyleus is **NOT** compatible with Python 3 (yet).

Quick Install
-------------

Install in a virtualenv:

.. code-block:: none

   $ virtualenv my_venv
   $ source my_venv/bin/activate
   $ pip install pyleus

Quick Start
-----------

Build an example:

.. code-block:: none

   $ git clone https://github.com/Yelp/pyleus.git
   $ pyleus build pyleus/examples/exclamation_topology/pyleus_topology.yaml

Run the example locally:

.. code-block:: none

   $ pyleus local exclamation_topology.jar

When you are done, hit ``C-C``.

Run the example on a Storm cluster:

.. code-block:: none

   $ pylues submit -n NIMBUS_IP exclamation_topology.jar

Documentation
-------------

.. toctree::
   :maxdepth: 2

   tutorial
   reliability
   grouping
   options
   parallelism
   tick
   logging
   install
   cli
   configuration

API Documentation
-----------------

.. toctree::
   :maxdepth: 4

   api

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _Apache Storm: https://storm.incubator.apache.org/
