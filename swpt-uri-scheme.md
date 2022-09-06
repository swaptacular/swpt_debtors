The `swpt` URI scheme
=====================

[Swaptacular] defines its own `swpt` [URI scheme]. URIs with this
scheme refer either to a concrete Swaptacular currency, or to a
concrete account in a given Swaptacular currency. For example:

* `swpt:1234` refers to the Swaptacular currency with debtor ID 1234.

* `swpt:1234/my-account` refers to the account "my-account" in the
  Swaptacular currency with debtor ID 1234.


References to Swaptacular currencies
------------------------------------

The general form is `swpt:<debtor-id-2c>`, where `<debtor-id-2c>` is
an integer (Base-10) between 0 and 18446744073709551615 (an unsigned
64-bits integer). For negative debtor IDs `<debtor-id-2c>` should be
the debtor ID's [two's complement]. For example, the URI for the
currency with debtor ID `0` is `swpt:0`, and the URI for the currency
with debtor ID `-1` is `swpt:18446744073709551615`.


References to concrete accounts
-------------------------------

The general form is `swpt:<debtor-id-2c>/<account-id-enc>`. Here, the
meaning of `<debtor-id-2c>` is the same as before, and
`<account-id-enc>` encodes the [account identifier] for the account,
as it is defined by the [Swaptacular Messaging Protocol].

The `<account-id-enc>` string uses a very simple encoding:

* If the first symbol in the string **is not** `"!""`, then the string
  contains the account identifier. In this case, the account
  identifier can contain only a limited set of symbols: ASCII letters,
  numbers, underscore, equal sign, minus sign (regular expression:
  `^[A-Za-z0-9_=-]*$`).

* If the first symbol in the string **is** `"!""`, then the rest of
  the string contains the account identifier, [Base64URL] encoded.


[Swaptacular]: https://swaptacular.github.io/overview
[URI scheme]: https://en.wikipedia.org/wiki/Uniform_Resource_Identifier#Syntax
[two's complement]: https://en.wikipedia.org/wiki/Two%27s_complement
[account identifier]: https://github.com/epandurski/swpt_accounts/blob/master/protocol.rst#account-id
[Swaptacular Messaging Protocol]: https://github.com/swaptacular/swpt_accounts/blob/master/protocol.rst
[Base64URL]: https://base64.guru/standards/base64url
