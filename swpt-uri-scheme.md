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
64-bits integer). For negative debtor IDs, `<debtor-id-2c>` is the
debtor ID's [two's complement]. For example, the URI for the currency
with debtor ID `0` is `swpt:0`; the URI for the currency with debtor
ID `-1` is `swpt:18446744073709551615`.


[Swaptacular]: https://swaptacular.github.io/overview
[URI scheme]: https://en.wikipedia.org/wiki/Uniform_Resource_Identifier#Syntax
[two's complement]: https://en.wikipedia.org/wiki/Two%27s_complement
