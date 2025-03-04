import logging
import re
from typing import Any, Dict, List, Optional, Tuple, Union

from couchbase.n1ql import N1QLQuery, QueryScanConsistency
from couchbase.options import QueryOptions
from couchbase.result import QueryResult

from .options import build_opts
from .session import Session
from .timeout import Timeout

_TIMEOUT = Timeout()


class N1ql:
    """A Couchbase SQL++ (N1QL) class making for easier and safer query building

    Args:
        session (implements :class:`~couchbase_helper.protocols.SessionProt`)
            The cluster connection session
        logger (:class:`logging.logger`):
            The logging instance to use for log message. Defaults to the root logger.

    Usage:
        The class is intended used as chained methods, example:
        ```
        from couchbase_helper import Session
        from couchbase_helper.n1ql import N1ql

        session = Session("localhost", "user", "password", bucket="test")
        n1ql = N1ql(session)

        # select all restaurants from the "businesses" bucket
        restaurants = n1ql.select("name").from_("businesses").where("type=", "restaurant").get()
        for restaurant in restaurants:
            ...

        # select all butcher shops and bakeries from the "businesses" bucket
        shops = n1ql.select("name").from_("businesses").where("type=", "butcher").or_where("type=", "bakery").get()
        for shop in shops:
            ...
        ```
    """

    def __init__(self, session: Session, logger: Optional[logging.Logger] = None):
        if logger is None:
            logger = logging.getLogger()
        self.logger = logger

        self.session = session

        if not self.session.connected:
            self.session.connect()

        # Query runtime variables
        self._columns = ["*"]
        self._conditions: List[Tuple[str, str, Any]] = []
        self._distinct: bool = False
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._session_reset: Dict[str, str] = {}

    def select(self, columns: Optional[Union[str, List[str]]] = None):
        """
        Set the column or columns to select

        Args:
            columns (Union[str, List[str]]):
                The column or columns to select in the query as a list of strings or comma separated string
        Returns:
            `self`
        """
        if columns is None:
            columns = ["*"]
        if isinstance(columns, str):
            columns = [col.strip() for col in columns.split(",")]

        self._columns = columns

        return self

    def distinct(self, enable: bool = True):
        """
        Will set query to select distinct values

        Args:
            enable (bool):
                Whether the distinct flag should be enabled (True) or not (False).
                This will always be reset to False when invoking the `get` method

        Returns:
            `self`
        """
        self._distinct = enable
        return self

    def from_(
        self,
        bucket: Optional[str] = None,
        scope: Optional[str] = None,
        collection: Optional[str] = None,
    ):
        if bucket is not None and bucket != self.session.bucket.name:
            self._session_reset["bucket"] = self.session.bucket.name
            self.session.bucket = bucket

        if scope is not None and scope != self.session.scope.name:
            self._session_reset["scope"] = self.session.scope.name
            self.session.scope = scope

        if collection is not None and collection != self.session.collection.name:
            self._session_reset["collection"] = self.session.collection.name
            self.session.collection = collection

        return self

    def where(self, key: str, value: Any):
        """
        Add an inclusive/and where condition

        Args:
            key (str):
                The key (and optionally operator) to add to the condition expression.
            value (Any):
                The value to compare with.

        Returns:
             `self`
        """
        self._where("AND", key, value)

        return self

    def or_where(self, key: str, value: Any):
        """
        Add an exclusive/or where condition

        Args:
            key (str):
                The key (and optionally operator) to add to the condition expression.
            value (Any):
                The value to compare with.

        Returns:
             `self`
        """
        self._where("OR", key, value)

        return self

    def orwhere(self, key: str, value: Any):
        """an alias for :class:`~.or_where`"""
        return self.or_where(key, value)

    # TODO ... : def not_(self, ...):

    def _where(self, type_: str, key: str, value: Any):
        """adds and/or where conditions"""
        cleaned_key = self._clean_key(key)
        column = self._enclose_reserved_word(cleaned_key)
        operator = None
        if value is None and not self._has_operator(key):
            operator = " IS NULL"
        if not self._has_operator(key) or not operator:
            operator = " ="

        processed_key = f"{column}{operator}"

        self._conditions.append((type_, processed_key, value))

    def limit(self, limit: int):
        """
        Add a limit to the query

        Args:
             limit (int):
                The limit of items a query can return

        Returns:
            `self`
        """
        try:
            self._limit = int(limit)
        except ValueError:
            pass

        return self

    def offset(self, offset: int):
        """
        Add an offset to the query

        Args:
             offset (int):
                The number of records the query must skip

        Returns:
            `self`
        """
        try:
            self._offset = int(offset)
        except ValueError:
            pass

        return self

    def skip(self, skip: int):
        """an alias for :class:`offset`"""
        return self.offset(skip)

    def rows(self, opts: Optional[QueryOptions] = None) -> Optional[QueryResult]:
        """
        Execute a select statement and return the fetched rows

        Args:
             opts (optional `couchbase.options.QueryOptions`):
                Any query options to use when executing the query.

        Returns:
            :class:`couchbase.result.QueryResult` iterable
        """
        # generate 'FROM' expression
        from_ = self._enclose_reserved_word(self.session.bucket.name)
        if (
            self.session.scope.name != "_default"
            or self.session.collection.name != "_default"
        ):
            from_ += f".{self._enclose_reserved_word(self.session.scope.name)}.{self._enclose_reserved_word(self.session.collection.name)}"

        ident = self.session.bucket.name[0:1]
        prefix = f"{ident}."

        # generate columns for selection
        columns = ", ".join(
            [
                f"{prefix if col != '*' else ''}{self._enclose_reserved_word(col)}"
                for col in self._columns
            ]
        )

        # generate where expression
        positional_arguments = []
        where = ""
        if self._conditions:
            for idx, condition in enumerate(self._conditions, 1):
                type_, key, value = condition
                positional_arguments.append(value)
                _where_part = f"{type_.upper() if len(where) else ''} {prefix}{key} ${idx}".strip()
                where += f"{_where_part} "
            where = f"WHERE {where}"

        # append limit
        limit = ""
        if self._limit is not None:
            limit = f"LIMIT {self._limit}"

        # append skip
        offset = ""
        if self._offset is not None:
            offset = f"OFFSET {self._offset}"

        # init QueryOptions
        if opts is None:
            opts = {}
        if positional_arguments:
            opts["positional_parameters"] = positional_arguments

        # generate the prepared statement
        statement = f"SELECT{' DISTINCT' if self._distinct else ''} {columns} FROM {from_} {ident} {where} {limit} {offset}".strip()

        # execute and return rows
        rows = None
        try:
            rows = self._execute(statement, opts).rows()
        except AttributeError:
            pass
        finally:
            self._reset()

        return rows

    def _execute(
        self,
        statement: str,
        opts: Optional[QueryOptions] = None,
        *,
        consistency: Optional[QueryScanConsistency] = None,
    ):
        """execute a statement"""
        # init QueryOptions
        if opts is None:
            opts = {}
        options = build_opts("query", opts=opts)

        try:
            # Initiate the N1QLQuery instance
            query = N1QLQuery(statement)
            if consistency is not None:
                query.consistency = consistency
            query.timeout = self.session.timeout.query
            return self.session.cluster.query(query.statement, **options)
        except Exception as _err:  # intentionally broad initially to see what exceptions are actually raised
            print(f"SQL++ exception happened ({type(_err).__name__}): {_err}")

        return None

    @staticmethod
    def _has_operator(string: str):
        """determine if a column/key has an operator"""
        return len(re.findall(r"(\s|<|>|!|=|is null|is not null)", string)) > 0

    @staticmethod
    def _clean_key(string):
        """clean a column/key from any non-word chars"""
        return re.sub(r"\W", "", string)

    def _reset(self):
        """resets class variables and session"""
        self._columns = ["*"]
        self._conditions = []
        self._distinct = False
        self._limit = None
        self._offset = None
        if self._session_reset:
            for prop, original in self._session_reset.items():
                setattr(self.session, prop, original)
            self._session_reset = {}

    @staticmethod
    def _enclose_reserved_word(word):
        # TODO: find a better place for this list ...
        """enclose reserved words"""
        reserved = (
            "_INDEX_CONDITION",
            "_INDEX_KEY",
            "ADVISE",
            "ALL",
            "ALTER",
            "ANALYZE",
            "AND",
            "ANY",
            "ARRAY",
            "AS",
            "ASC",
            "AT",
            "BEGIN",
            "BETWEEN",
            "BINARY",
            "BOOLEAN",
            "BREAK",
            "BUCKET",
            "BUILD",
            "BY",
            "CACHE",
            "CALL",
            "CASE",
            "CAST",
            "CLUSTER",
            "COLLATE",
            "COLLECTION",
            "COMMIT",
            "COMMITTED",
            "CONNECT",
            "CONTINUE",
            "CORRELATED",
            "COVER",
            "CREATE",
            "CURRENT",
            "CYCLE",
            "DATABASE",
            "DATASET",
            "DATASTORE",
            "DECLARE",
            "DECREMENT",
            "DEFAULT",
            "DELETE",
            "DERIVED",
            "DESC",
            "DESCRIBE",
            "DISTINCT",
            "DO",
            "DROP",
            "EACH",
            "ELEMENT",
            "ELSE",
            "END",
            "ESCAPE",
            "EVERY",
            "EXCEPT",
            "EXCLUDE",
            "EXECUTE",
            "EXISTS",
            "EXPLAIN",
            "FALSE",
            "FETCH",
            "FILTER",
            "FIRST",
            "FLATTEN",
            "FLATTEN_KEYS",
            "FLUSH",
            "FOLLOWING",
            "FOR",
            "FORCE",
            "FROM",
            "FTS",
            "FUNCTION",
            "GOLANG",
            "GRANT",
            "GROUP",
            "GROUPS",
            "GSI",
            "HASH",
            "HAVING",
            "IF",
            "IGNORE",
            "ILIKE",
            "IN",
            "INCLUDE",
            "INCREMENT",
            "INDEX",
            "INFER",
            "INLINE",
            "INNER",
            "INSERT",
            "INTERSECT",
            "INTO",
            "IS",
            "ISOLATION",
            "JAVASCRIPT",
            "JOIN",
            "KEY",
            "KEYS",
            "KEYSPACE",
            "KNOWN",
            "LANGUAGE",
            "LAST",
            "LATERAL",
            "LEFT",
            "LET",
            "LETTING",
            "LEVEL",
            "LIKE",
            "LIMIT",
            "LSM",
            "MAP",
            "MAPPING",
            "MATCHED",
            "MATERIALIZED",
            "MAXVALUE",
            "MERGE",
            "MINVALUE",
            "MISSING",
            "NAMESPACE",
            "NEST",
            "NEXT",
            "NEXTVAL",
            "NL",
            "NO",
            "NOT",
            "NTH_VALUE",
            "NULL",
            "NULLS",
            "NUMBER",
            "OBJECT",
            "OFFSET",
            "ON",
            "OPTION",
            "OPTIONS",
            "OR",
            "ORDER",
            "OTHERS",
            "OUTER",
            "OVER",
            "PARSE",
            "PARTITION",
            "PASSWORD",
            "PATH",
            "POOL",
            "PRECEDING",
            "PREPARE",
            "PREV",
            "PREVIOUS",
            "PREVVAL",
            "PRIMARY",
            "PRIVATE",
            "PRIVILEGE",
            "PROBE",
            "PROCEDURE",
            "PUBLIC",
            "RANGE",
            "RAW",
            "READ",
            "REALM",
            "RECURSIVE",
            "REDUCE",
            "RENAME",
            "REPLACE",
            "RESPECT",
            "RESTART",
            "RESTRICT",
            "RETURN",
            "RETURNING",
            "REVOKE",
            "RIGHT",
            "ROLE",
            "ROLLBACK",
            "ROW",
            "ROWS",
            "SATISFIES",
            "SAVEPOINT",
            "SCHEMA",
            "SCOPE",
            "SELECT",
            "SELF",
            "SEQUENCE",
            "SET",
            "SHOW",
            "SOME",
            "START",
            "STATISTICS",
            "STRING",
            "SYSTEM",
            "THEN",
            "TIES",
            "TO",
            "TRAN",
            "TRANSACTION",
            "TRIGGER",
            "TRUE",
            "TRUNCATE",
            "UNBOUNDED",
            "UNDER",
            "UNION",
            "UNIQUE",
            "UNKNOWN",
            "UNNEST",
            "UNSET",
            "UPDATE",
            "UPSERT",
            "USE",
            "USER",
            "USERS",
            "USING",
            "VALIDATE",
            "VALUE",
            "VALUED",
            "VALUES",
            "VECTOR",
            "VIA",
            "VIEW",
            "WHEN",
            "WHERE",
            "WHILE",
            "WINDOW",
            "WITH",
            "WITHIN",
            "WORK",
            "XOR",
        )

        if word.upper() in reserved:
            word = f"`{word}`"

        if "." in word:
            word = f"`{word}`"

        return word
