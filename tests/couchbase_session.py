from couchbase_helper import CouchbaseHelper
from couchbase_helper.fake import Session


session = Session(
    hostname="localhost", username="test", password="testtest", bucket="test"
)
_INSTANCE = None


def instance():
    global _INSTANCE
    if _INSTANCE is None:
        _INSTANCE = CouchbaseHelper(session=session)

    return _INSTANCE
