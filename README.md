couchbase-helper: A simple helper package for Couchbase operations
=======================================

[![pypi package](https://img.shields.io/pypi/v/couchbase-helper?color=%2334D058&label=pypi%20package)](https://pypi.org/project/couchbase-helper/)
[![downloads](https://img.shields.io/pypi/dm/couchbase-helper)](https://pypi.org/project/couchbase-helper/)
[![versions](https://img.shields.io/pypi/pyversions/couchbase-helper.svg?color=%2334D058)](https://pypi.org/project/couchbase-helper/)
![Tests coverage](./coverage.svg)
[![licence](https://img.shields.io/pypi/l/couchbase-helper)](./LICENSE)

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
* `replace`
* `replace_multi`
* `get`
* `get_multi`
* `remove`
* `remove_multi`

It currently also has very basic functionality for view queries (`view_query`) which isn't all that fancy. Follow issue
[#18](https://github.com/sitzz/python-couchbase-helper/issues/18) for updates on this.

An N1ql (SQL++) helper class is also available, which provides chainable methods to create SQL++ queries to select from
indexes.

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

# Examples

**Basic operations**
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

# Replace a document
new_document = {
    "bar": "baz"
}
cb.replace("foo1", new_document)
```

**SQL++/N1QL examples**
```Python
from couchbase_helper import Session
from couchbase_helper.n1ql import N1ql

# Connect to Couchbase server/cluster
session = Session(
    hostname="localhost",
    username="username",
    password="password",
    bucket="bucket",
    timeout=10,
)
session.connect()
n1ql = N1ql(session=session)

# Select documents where foo=bar
rows = n1ql.where("foo=", "bar").rows()
for row in rows:
    ...

# You can also select specific columns, from a different bucket , scope, or even collection than the session's:
rows = n1ql.select("callsign").from_("travel-sample", "inventory", "airport").where("city=", "San Jose").or_where("city=", "New York").rows()
for row in rows:
    ...
```
