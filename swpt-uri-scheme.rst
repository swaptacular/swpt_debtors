+++++++++++++++++++++++++++++++++++
``swpt`` URI scheme for Swaptacular
+++++++++++++++++++++++++++++++++++
:Description: Specifies the "swpt" URI scheme for Swaptacular
:Author: Evgeni Pandurksi
:Contact: epandurski@gmail.com
:Date: 2022-09-06
:Version: 1.0
:Copyright: This document has been placed in the public domain.


Overview
========

This document specifies a new ``swpt`` `URI scheme`_.

`Swaptacular`_ uses the ``swpt`` URI scheme to refer either to a
concrete Swaptacular currency, or to a concrete account with a given
debtor (currency). For example:

* ``swpt:1234`` refers to the Swaptacular currency with debtor
  ID 1234.

* ``swpt:1234/example-account`` refers to the account
  "example-account" with debtor 1234.

**Note:** The key words "MUST", "MUST NOT", "REQUIRED", "SHALL",
"SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in
RFC 2119.


References to Swaptacular currencies
------------------------------------

The general form is ``swpt:<debtor-id-2c>``, where ``<debtor-id-2c>``
is an integer (Base-10) between 0 and 18446744073709551615 (an
unsigned 64-bits integer). For negative debtor IDs ``<debtor-id-2c>``
MUST be the debtor ID's `two's complement`_. For example, the URI for
the currency with debtor ID ``0`` is ``swpt:0``, and the URI for the
currency with debtor ID ``-1`` is ``swpt:18446744073709551615``.


References to concrete accounts
-------------------------------

The general form is ``swpt:<debtor-id-2c>/<account-id-enc>``. Here,
the meaning of ``<debtor-id-2c>`` is the same as before, and
``<account-id-enc>`` encodes the `account identifier`_ for the
account, defined by the `Swaptacular Messaging Protocol`_.

The ``<account-id-enc>`` string uses a very simple encoding:

* If the first symbol in the string **is not** ``"!"``, then the
  string contains the account identifier. In this case, the account
  identifier MUST contain only a limited set of symbols: ASCII
  letters, numbers, underscore, equal sign, minus sign (regular
  expression: ``^[A-Za-z0-9_=-]*$``).

* If the first symbol in the string **is** ``"!"``, then the rest of
  the string contains the account identifier, `Base64URL`_ encoded.

**Important note:** When the account identifier contains only a
limited set of symbols (regular expression: ``^[A-Za-z0-9_=-]*$``),
the ``<account-id-enc>`` string MUST be equal to the account
identifier, and Base64URL encoding MUST NOT be used.



.. _Swaptacular: https://swaptacular.github.io/overview
.. _URI scheme: https://en.wikipedia.org/wiki/Uniform_Resource_Identifier#Syntax
.. _two's complement: https://en.wikipedia.org/wiki/Two%27s_complement
.. _account identifier: https://github.com/epandurski/swpt_accounts/blob/master/protocol.rst#account-id
.. _Swaptacular Messaging Protocol: https://github.com/swaptacular/swpt_accounts/blob/master/protocol.rst
.. _Base64URL: https://base64.guru/standards/base64url
