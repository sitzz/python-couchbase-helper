from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from couchbase_helper import CouchbaseHelper, Session
from couchbase_helper.n1ql import N1ql
from fake_couchbase.cluster import Cluster as FakeCluster

username = "test"
password = "testtest"
options = ClusterOptions(authenticator=PasswordAuthenticator(username, password))
session = Session(
    hostname="localhost", username=username, password=password, bucket="test"
)
session.cluster = FakeCluster(session.connection_string, options)
_CB_INSTANCE = None
_N1QL_INSTANCE = None


def cb_instance() -> CouchbaseHelper:
    global _CB_INSTANCE
    if _CB_INSTANCE is None:
        _CB_INSTANCE = CouchbaseHelper(session=session)

    return _CB_INSTANCE


def n1ql_instance() -> N1ql:
    global _N1QL_INSTANCE
    if _N1QL_INSTANCE is None:
        _N1QL_INSTANCE = N1ql(session=session)

    return _N1QL_INSTANCE
