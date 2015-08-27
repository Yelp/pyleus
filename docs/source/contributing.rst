.. contibuting:

Contributing
============

Everyone is encouraged to contribute to Pyleus. You can contribute by forking the `GitHub repo`_ and making a pull request or opening an issue.

.. note::

   Please run ``make``, ``make test`` and ``make docs`` to verify that everything works as expected **before** submitting a pull request.

A pull request will be merged to the `develop`_ branch by a committer after it receives at least two `+1` from two different committers. In case of urgent pull requests (e. g. fixing a disruptive bug right after a new release), a single committer can approve and merge them.

Manual testing
--------------
At the current state of the project, Pyleus does have tests covering Python code, while the Java code is lacking automated tests.

For this reason, we kindly ask you to manually test your changes if they involve changes to the Java code or at least to ask a project committer to do that. We are working for improving the situation as soon as possible.

You can use topologies living in the `examples`_ folder for manual testing.

Repo organization
-----------------

`master`_ branch has the latest stable release, while all development is carried on the `develop`_ branch. You can find all previous releases on `releases`_ page on GitHub.

New releases are planned and managed by committers, but you are welcome to participate or ask for one.

Versioning
----------

Pyleus version numbers look like ``MAJOR.MINOR.PATH``. While Pyleus ``MAJOR < 1``, the software has to be considered in **beta** and therefore the following rules apply:

1. A ``MINOR`` version change should be considered significant and might introduce non-backward-compatible changes
2. A ``PATCH`` version change may add functionality in a backward-compatible manner and/or bug fixes and/or minor improvements (e.g., fixing bugs, performance improvements, code cleanup/refactoring)

We do not increment versions for documentation and other non-code changes.

.. _development_tips:

Development tips
----------------

If you are willing to contribute to Pyleus, you need to know that Pyleus **automatically adds itself as a dependency to your topology under the hood**.

This means that if you change something in ``topology_builder`` or ``pyleus/storm``, you probably will still run topologies including the Pyleus code available on PyPI, instead of the one that you just modified.

A workaround to the problem may be to run a local PyPI server from the folder where the modified Pyleus package lives and use the canonical PyPI server as fallback for all other Python packages:

.. code-block:: bash

   $ virtualenv mylocalpypi
   $ source bin/activate/mylocalpypi
   $ pip install pypiserver
   $ pypi-server -p 7778 --fallback-url https://pypi.python.org/simple/ pyleus/dist/

Then write a ``.pyleus.conf`` like the following and put it in your home directory:

.. code-block:: ini

   [build]
   pypi_index_url: http://0.0.0.0:7778/simple/


In this way, after any change you do, you just need to

.. code-block:: bash

   make clean && make all

in order to have your updated Pyleus code installed into your topologies.

.. tip::

   If you are using ``pip >= 7.0.0``, you may need to specify ``trusted-host: 0.0.0.0`` in your ``pip.conf`` `file`_ (read `pip docs`_ for more info).

If you modify ``topology_builder`` or you want to make changes to ``pyleus/cli``, you may also need to uninstall and reinstall Pyleus in your development environment.

.. note::

   To verify you are including the right Pyleus code, you may just ``vim your_topology.jar`` and look for the changes you made.

.. _GitHub repo: https://github.com/Yelp/pyleus
.. _examples: https://github.com/Yelp/pyleus/tree/develop/examples
.. _develop: https://github.com/Yelp/pyleus/tree/develop
.. _master: https://github.com/Yelp/pyleus/tree/master
.. _releases: https://github.com/Yelp/pyleus/releases
.. _pip docs: https://pip.pypa.io/en/latest/news.html
.. _file: https://pip.pypa.io/en/latest/user_guide.html?highlight=conf#config-file
