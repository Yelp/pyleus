.. _install:

Install
=======

You can install pyleus from PyPI either system-wide or in a virtualenv:

.. code-block:: none

   $ pip install pyleus

.. note::

   You do **NOT** need to install pyleus on your Storm cluster.


   However, if you are going to use ``system_site_packages: true`` in your config file, you should be aware that the environment of your Storm nodes needs to match the one on the machine used for building the topology. This means you actually **have to install** pyleus on your Storm cluster in this case.

.. warning::

   Installing Pyleus from source is **NOT** recommended. Please refer to section :ref:`development_tips` for more info.

   The **recommended** way to install Pyleus is ``pip install pyleus`` either system-wide or in a virtualenv.

Specify your Storm path
-----------------------

Pyleus will automatically look for the ``storm`` executable in your ``$PATH`` when executing a command.

If you do not have Apache Storm already installed on your machine, you will need to download and extract Storm 0.9.4 from https://storm.apache.org/downloads.html.

After that, create a config file ``~/.pyleus.conf`` so Pyleus can find the ``storm`` command:

.. code-block:: ini

   [storm]
   storm_cmd_path: /path/to/apache-storm-0.9.4/bin/storm
