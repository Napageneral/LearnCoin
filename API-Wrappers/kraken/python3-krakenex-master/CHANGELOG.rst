python3-krakenex change log
===========================

All notable changes should be documented in this file.

The format is based on `Keep a Changelog`_, and this project adheres
to `semantic versioning`_.

.. _Keep a Changelog: http://keepachangelog.com/
.. _semantic versioning: http://semver.org/

[v1.0.0a0] - 2017-07-02 (Sunday)
--------------------------------

**Internal alpha testing release!**

Not for general use. For that reason, ``pip`` package not provided.

Added
^^^^^
* More examples.

Changed
^^^^^^^
* Connection now raises ``http.client.HTTPException`` if response
  doesn't have ``20x`` status code. (`#17`_)
* Fix new connection thrashing if one is not provided for reuse
  (as was described in the docs). (`#27`_)
* Be explicit when using default arguments in functions that have
  optional ones. (`#19`_)
* Renamed ``NEWS`` to ``CHANGELOG``.

Deprecated
^^^^^^^^^^
* ``krakenex.API.set_connection()`` method. Access ``krakenex.API.conn``
  attribute directly.

.. _#17: https://github.com/veox/python3-krakenex/pull/17
.. _#19: https://github.com/veox/python3-krakenex/issues/19
.. _#27: https://github.com/veox/python3-krakenex/issues/27

[v0.1.4] - 2017-03-27 (Monday)
------------------------------

Changed
^^^^^^^
* Properly release key file descriptor after reading in key. (`#7`_)
* Verbose docs, served at ``https://python3-krakenex.readthedocs.io/``.

.. _#7: https://github.com/veox/python3-krakenex/pull/17

[v0.1.3] - 2017-01-31 (Tuesday)
-------------------------------
  
Changed
^^^^^^^
* Single-source version and URL - used during setup and in
  ``User-Agent``. (`#5`_)

.. _#5: https://github.com/veox/python3-krakenex/issues/5

[v0.1.2] - 2016-11-05 (Saturday)
--------------------------------

Changed
^^^^^^^
* Ship examples with PyPI package.

[v0.1.1] - 2016-11-05 (Saturday)
--------------------------------

Changed
^^^^^^^
* Renamed README and LICENSE according to PyPI recommendations.

[v0.1.0] - 2016-10-31 (Monday)
------------------------------

Added
^^^^^
* Now available on `PyPI`_ as a source distribution. (`#3`_)

.. _PyPI: https://pypi.python.org/pypi/krakenex
.. _#3: https://github.com/veox/python3-krakenex/issues/3

Changed
^^^^^^^
* Change versioning scheme to semantic versioning (recommended by PyPI).

[v0.0.6.2] - 2016-04-18 (Monday)
--------------------------------

Added
^^^^^
* Basic documentation with sphinx.

[v0.0.6.1] - 2016-03-25 (Friday)
--------------------------------

Changed
^^^^^^^
* Classes sub-classed from ``object``.

[v0.0.6] - 2014-07-22 (Tuesday)
-------------------------------

Changed
^^^^^^^
* Core license changed from GPLv3 to LGPLv3. Examples remain at Simplified BSD.

[v0.0.5] - 2014-05-01 (Thursday)
--------------------------------

Added
^^^^^
* ``API.set_connection()`` method to set default connection.

[v0.0.4.1] - 2014-04-30 (Wednesday)
-----------------------------------

Changed
^^^^^^^
* Fixed ``User-Agent`` still reporting version ``0.0.3``.

[v0.0.4] - 2014-04-11 (Friday)
------------------------------

Added
^^^^^
* ``conditional-close`` example.
* Examples licensed under the Simplified BSD license.

Changed
^^^^^^^
* Original Python 2 version ported to Python 3.

[v0.0.3] - 2014-01-10 (Friday)
------------------------------

Added
^^^^^
* ``API.load_key()`` method to allow loading key/secret pair from file.

[v0.0.2] - 2014-01-04 (Saturday)
--------------------------------

Added
^^^^^
* Basic implementation of ``KrakenConnection`` class.
* Optional ``conn`` argument to query methods allows connection reuse.

[v0.0.1] - 2013-12-13 (Wednesday)
---------------------------------

Added
^^^^^
* Basic ``API`` class with ``query_{public,private}()`` methods.
* Licensed under GPLv3.
