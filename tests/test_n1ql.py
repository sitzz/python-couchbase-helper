from couchbase_helper.n1ql import N1ql

from .couchbase_session import n1ql_instance


##################################################
# Test instance                                  #
##################################################
def test_instance():
    assert isinstance(n1ql_instance(), N1ql)


##################################################
# Test select                                    #
##################################################
def test_select_default():
    assert n1ql_instance()._columns == ["*"]


def test_select_custom_string():
    n1ql_instance().select("kings,queens")
    assert n1ql_instance()._columns == ["kings", "queens"]


def test_select_custom_list():
    n1ql_instance().select(["horses", "donkeys"])
    assert n1ql_instance()._columns == ["horses", "donkeys"]


##################################################
# Test distinct                                  #
##################################################
def test_distinct():
    n1ql_instance().distinct()
    assert n1ql_instance()._distinct


def test_not_distinct():
    n1ql_instance().distinct(False)
    assert not n1ql_instance()._distinct


##################################################
# Test from_                                     #
##################################################
def test_from_bucket():
    bucket_name = "bucketname"
    n1ql_instance().from_(bucket_name)
    assert n1ql_instance().session.bucket.name == bucket_name
    n1ql_instance()._reset()


def test_from_bucket_scope():
    bucket_name = "bucketname2"
    scope_name = "scope2"
    n1ql_instance().from_(bucket_name, scope_name)
    assert n1ql_instance().session.bucket.name == bucket_name
    assert n1ql_instance().session.scope.name == scope_name
    n1ql_instance()._reset()


def test_from_bucket_scope_collection():
    bucket_name = "bucketname3"
    scope_name = "scope3"
    collection_name = "collection3"
    n1ql_instance().from_(bucket_name, scope_name, collection_name)
    assert n1ql_instance().session.bucket.name == bucket_name
    assert n1ql_instance().session.scope.name == scope_name
    assert n1ql_instance().session.collection.name == collection_name


##################################################
# Test where                                     #
##################################################
def test_where():
    n1ql_instance().where("type=", "random")
    assert n1ql_instance()._conditions == [("AND", "type =", "random")]


def test_where_or():
    n1ql_instance().or_where("type=", "not-random")
    assert n1ql_instance()._conditions == [
        ("AND", "type =", "random"),
        ("OR", "type =", "not-random"),
    ]


##################################################
# Test limit                                     #
##################################################
def test_limit():
    n1ql_instance().limit("11")
    assert n1ql_instance()._limit == 11
    n1ql_instance().limit(10)
    assert n1ql_instance()._limit == 10
    n1ql_instance().limit("invalid-string")
    assert n1ql_instance()._limit == 10


##################################################
# Test offset/skip                               #
##################################################
def test_offset():
    n1ql_instance().offset(3)
    assert n1ql_instance()._offset == 3
    n1ql_instance().offset("4")
    assert n1ql_instance()._offset == 4
    n1ql_instance().offset("invalid-string")
    assert n1ql_instance()._offset == 4


##################################################
# Test rows                                      #
##################################################
def test_rows():
    assert n1ql_instance().rows() is None


##################################################
# Test reset                                     #
##################################################
def test_reset():
    assert n1ql_instance()._columns == ["*"]
    assert not n1ql_instance()._distinct
    assert n1ql_instance().session.bucket.name == "test"
    assert n1ql_instance().session.scope.name == "_default"
    assert n1ql_instance().session.collection.name == "_default"
    assert n1ql_instance()._conditions == []
    assert n1ql_instance()._limit is None
    assert n1ql_instance()._offset is None
