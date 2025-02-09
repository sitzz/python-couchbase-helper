from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from couchbase_helper import CouchbaseHelper
from couchbase_helper import Session
from fake_couchbase.cluster import Cluster as FakeCluster

username = "test"
password = "testtest"
options = ClusterOptions(authenticator=PasswordAuthenticator(username, password))
session = Session(
    hostname="localhost", username=username, password=password, bucket="test"
)
session.cluster = FakeCluster(session.connection_string, options)
_INSTANCE = None


def instance():
    global _INSTANCE
    if _INSTANCE is None:
        _INSTANCE = CouchbaseHelper(session=session)

    return _INSTANCE
