.. _install:

Install
=======

You can install pyleus from PyPI either system-wide or in a virtualenv:

.. code-block:: none

   $ pip install pyleus

.. note::

   You do **NOT**  need to install pyleus on your Storm cluster.

.. warning::

   Installing Pyleus from source is **NOT** recommended. Please refer to section :ref:`development_tips` for more info.

Specify your Storm path
-----------------------

Pyleus will automatically look for the ``storm`` executable in your ``$PATH`` when executing a command.

If you do not have Apache Storm already installed on your machine, you will need to download and extract Storm 0.9.2-incubating—the current release—from https://storm.apache.org/downloads.html.

After that, create a config file ``~/.pyleus.conf`` so Pyleus can find the ``storm`` command:

.. code-block:: ini

   [storm]
   storm_cmd_path: /path/to/apache-storm-0.9.2-incubating/bin/storm
