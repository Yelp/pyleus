.. Pyleus documentation master file, created by
   sphinx-quickstart on Tue Sep 30 08:01:12 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Pyleus |version|
================

Pyleus is a Python 2.6+ layer built on top of `Apache Storm`_ making possible to write, build and manage pure Python Storm topologies in a pythonic way.

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

   $ pylues submit -s STORM_CLUSTER_IP exclamation_topology.jar

Documentation
-------------

.. toctree::
   :maxdepth: 2

   tutorial
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
