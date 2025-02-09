couchbase-helper: A simple helper package for Couchbase operations
=======================================

<!--[![badge](https://img.shields.io/pypi/v/couchbase-helper)](https://pypi.org/project/couchbase-helper/)-->
<!--[![badge](https://img.shields.io/pypi/dm/couchbase-helper)](https://pypi.org/project/couchbase-helper/)-->
<!--[![badge](https://img.shields.io/pypi/l/couchbase-helper)](./LICENSE)-->

--------------------

# Intro

While the [Couchbase](https://pypi.org/project/couchbase/) packages is easy enough to use, sometimes it also just
requires some boilerplate code which becomes tiring to duplicate every time you need to perform simple document
operations. At least, that's what I thought. So, I started creating a simple re-usable script which later became a class
and ... well, here's Couchbase Helper!

Couchbase Helper is currently rather basic and mostly/only support the following operations fully:
* `insert`
* `insert_multi`
* `upsert`
* `upsert_multi`
* `get`
* `get_multi`
* `remove`
* `remove_multi`

It currently also has very basic functionality for view queries (`view_query`) and n1ql (`n1ql`) which aren't that
fancy. There are ideas on how to enhance these, especially the `n1ql`  method, but still nothing specific. Follow issues
[#9](https://github.com/sitzz/python-couchbase-helper/issues/9) and
[#18](https://github.com/sitzz/python-couchbase-helper/issues/18) for updates on this.

# Installation

Installation is as you're used to, using your favorite package manager:
```console
$ pip install couchbase-helper
... or
$ uv add couchbase-helper
... or
$ poetry add couchbase-helper
... and so on
```

# Example

```Python
from couchbase_helper import CouchbaseHelper, Session

# Connect to Couchbase server/cluster
session = Session(
    hostname="localhost",
    username="username",
    password="password",
    bucket="bucket",
    timeout=10,
)
cb = CouchbaseHelper(
    session=session,
)

# Insert document
document = {
    "foo": "bar"
}
cb.insert("foo1", document)

# Get document
document = cb.get("foo1")
print(document["foo"])  # bar

# Update document
document["hello"] = "world"
cb.upsert("foo1", document)
```


