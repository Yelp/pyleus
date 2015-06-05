0.3.0 (June 5, 2015)
--------------------

BACKWARDS INCOMPATIBILITIES:

* Storm < 0.9.3 no longer supported.

IMPROVEMENTS:

* Storm 0.9.4 support.

BUG FIXES:

* Fix ClassCastException in java code. [GH-94]

0.2.4 (December 10, 2014)
------------------

BUG FIXES:

* Don't delete system-installed base jar on build. [GH-74]

0.2.3 (December 8, 2014)
------------------

FEATURES:

* Specify Python interpreter as yaml option. [GH-54]

IMPROVEMENTS:

* Preliminary Python 3 support. [GH-15]
* Updates to documentation.

BUG FIXES:

* Fix project URL in ``setup.py``. [GH-49]
* Fix loss of precision during Java serialization. [GH-53]
* Fix call to traceback.format_exc. [GH-56]
* Fix logger name for LineSpout in /examples/logging. [GH-69]

0.2.2 (October 24, 2014)
------------------------

IMPROVEMENTS:

* Supplant ``--nimbus`` option with ``--nimbus-host`` and ``--nimbus-port``. [GH-35]
* Miscellaneous Python 3 compatibility improvements. [GH-37]
* Updates to ``README.rst`` and documentation.

BUG FIXES:

* Fix long integer serialization issue. [GH-48]

0.2.1 (October 17, 2014)
------------------------

IMPROVEMENTS:

* Rename ``--nimbus-ip`` option to ``--nimbus``. [GH-2]
* Improve Java code style. [GH-4]
* Add explicit dependency on ``virtualenv``. [GH-21]
* Updates to README.rst and documentation.

BUG FIXES:

* Fix ``pyleus build`` on case-insensitive filesystems. [GH-22]
* Fix installation of base JAR on some systems. [GH-24]

0.2 (October 15, 2014)
----------------------

* Initial open-source release
