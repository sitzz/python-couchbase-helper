from datetime import timedelta
import logging
from typing import Any, Dict, List, Optional, Union

from couchbase.diagnostics import ServiceType
from couchbase.exceptions import DocumentExistsException, DocumentNotFoundException
from couchbase.n1ql import N1QLQuery, QueryScanConsistency
from couchbase.options import (
    InsertMultiOptions,
    InsertOptions,
    GetMultiOptions,
    GetOptions,
    QueryOptions,
    RemoveMultiOptions,
    RemoveOptions,
    UpsertMultiOptions,
    UpsertOptions,
    ViewOptions,
    WaitUntilReadyOptions,
)
from couchbase.result import QueryResult, GetResult, MultiGetResult

from .protocols import SessionProt
from ._types import JSONType


class CouchbaseHelper:
    """A couchbase helper class to simplify document operations

    Args:
        session (implements :class:`~couchbase_helper.protocols.SessionProt`):
            The cluster connection session
        logger (:class:`logging.logger`):
            The logging instance to use for log message. Defaults to the root logger.
    """

    def __init__(
        self,
        session: SessionProt,
        logger: Optional[logging.Logger] = None,
    ):
        if logger is None:
            logger = logging.getLogger()
        self.logger = logger

        self.session = session

        if not self.session.connected:
            self.session.connect()

    def insert(
        self,
        key: str,
        value: JSONType,
        expiry: Optional[Union[int, timedelta]] = None,
        opts: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Insert a single document. Will fail if document already exists.

        Args:
            key (str):
                The key of the document to save.
            value (`~couchbase_helper._types.JSONType`):
                The value of the document to save.
            expiry (int | `:class:`datetime.timedelta`):
                The expiry of the document to save.
            opts (Dict[str, Any]):
                The operation options to use when saving document.

        Returns:
            (bool):
                The status of the insert operation.
        """
        args = {
            "key": key,
            "value": value,
            "opts": self._build_opts("insert", opts=opts, expiry=expiry),
        }

        try:
            self.session.cluster.wait_until_ready(
                timedelta(self.session.timeout.kv),
                WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
            )
            self.session.collection.insert(**args)
            return True
        except DocumentExistsException:
            return False

    def insert_multi(
        self,
        documents: Dict[str, JSONType],
        expiry: Optional[Union[int, timedelta]] = None,
        opts: Optional[Dict[str, Any]] = None,
        per_key_opts: Optional[Dict[str, InsertOptions]] = None,
    ):
        """Insert multiple documents, for each key-value pair in the `documents` dictionary
        a document will be created.

        Args:
            documents (Dict[str, JSONType]):
                A dictionary of the documents to be saved.
            expiry (int | `:class:`datetime.timedelta`):
                The expiry of the documents to save.
            opts (Dict[str, Any]):
                The operation options to use when saving document.
            per_key_opts (Dict[str, :class:`couchbase.options.InsertOptions`]):
                A dictionary of :class:`couchbase.options.InsertOptions` per document key.

        Returns:
            (bool):
                The status of the insert operations. Will return `True` if all operations
                were successful, `False` otherwise.
        """
        if opts is None:
            opts = {}

        if per_key_opts is not None:
            for key, val in per_key_opts.items():
                per_key_opts[key] = self._build_opts("insert", opts=val)

            opts["per_key_options"] = per_key_opts

        args = {
            "keys_and_docs": documents,
            "opts": self._build_opts("insert_multi", opts=opts, expiry=expiry),
        }
        try:
            self.session.cluster.wait_until_ready(
                timedelta(self.session.timeout.kv),
                WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
            )
            result = self.session.collection.insert_multi(**args)
            if result.all_ok:
                return True

            for key, exception in result.exceptions.items():
                self.logger.error("unable to add document %s: %s", key, exception)
        except Exception as _err:
            self.logger.error(
                "unhandled exception (%s): %s", type(_err).__name__, _err.args[0]
            )

        return False

    def upsert(
        self,
        key: str,
        value: JSONType,
        expiry: Optional[Union[int, timedelta]] = None,
        opts: Optional[Dict[str, Any]] = None,
    ):
        """Update or insert a single document.

        Args:
            key (str):
                The key of the document to save.
            value (`~couchbase_helper._types.JSONType`):
                The value of the document to save.
            expiry (int | `:class:`datetime.timedelta`):
                The expiry of the document to save.
            opts (Dict[str, Any]):
                The operation options to use when saving document.

        Returns:
            (bool):
                The status of the insert operation.
        """
        args = {
            "key": key,
            "value": value,
            "opts": self._build_opts("insert", opts=opts, expiry=expiry),
        }

        try:
            self.session.cluster.wait_until_ready(
                timedelta(self.session.timeout.kv),
                WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
            )
            self.session.collection.upsert(**args)
            return True
        except DocumentNotFoundException:
            return False

    def upsert_multi(
        self,
        documents: Dict[str, JSONType],
        expiry: Optional[Union[int, timedelta]] = None,
        opts: Optional[Dict[str, Any]] = None,
        per_key_opts: Optional[Dict[str, UpsertOptions]] = None,
    ) -> bool:
        """Update or insert multiple documents, for each key-value pair in the
        `documents` dictionary a document will be updated or created.

        Args:
            documents (Dict[str, JSONType]):
                A dictionary of the documents to be saved.
            expiry (int | `:class:`datetime.timedelta`):
                The expiry of the documents to save.
            opts (Dict[str, Any]):
                The operation options to use when saving document.
            per_key_opts (Dict[str, :class:`couchbase.options.UpsertOptions`]):
                A dictionary of :class:`couchbase.options.UpsertOptions` per document key.

        Returns:
            (bool):
                The status of the upsert operations. Will return `True` if all operations
                were successful, `False` otherwise.
        """
        if opts is None:
            opts = {}

        if per_key_opts is not None:
            for key, val in per_key_opts.items():
                per_key_opts[key] = self._build_opts("upsert", opts=val)

            opts["per_key_options"] = per_key_opts

        args = {
            "keys_and_docs": documents,
            "opts": self._build_opts("upsert_multi", opts=opts, expiry=expiry),
        }
        try:
            self.session.cluster.wait_until_ready(
                timedelta(self.session.timeout.kv),
                WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
            )
            result = self.session.collection.upsert_multi(**args)
            if result.all_ok:
                return True

            for key, exception in result.exceptions.items():
                self.logger.error("unable to add document %s: %s", key, exception)
        except Exception as _err:
            self.logger.error(
                "unhandled exception (%s): %s", type(_err).__name__, _err.args[0]
            )

        return False

    def get(
        self, key: str, opts: Optional[Dict[str, Any]] = None, *, raw: bool = False
    ) -> Optional[Union[GetResult, Dict[Any, Any]]]:
        """Get a single document

        Args:
            key (str):
                The key of the document to fetch.
            opts (Dict[str, Any]):
                The operation options to use when fetching the document.
            raw (bool):
                Whether to return the raw Couchbase response. Will return only the value
                as a dictionary otherwise.

        Returns:
            :class:`couchbase.result.GetResult` | Dict[Any, Any] | None
        """
        args = {"key": key, "opts": self._build_opts("get", opts=opts)}

        try:
            self.session.cluster.wait_until_ready(
                timedelta(self.session.timeout.kv),
                WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
            )
            document = self.session.collection.get(**args)
            return document if raw else document.content_as[dict]
        except DocumentNotFoundException:
            return None

    def get_multi(
        self,
        keys: List[str],
        opts: Optional[Dict[str, Any]] = None,
        *,
        raw: bool = False,
    ) -> Optional[Union[MultiGetResult, List[Dict[Any, Any]]]]:
        """Get multiple document

        Args:
            keys (List[str]):
                The key of the document to fetch.
            opts (Dict[str, Any]):
                The operation options to use when fetching the documents.
            raw (bool):
                Whether to return the raw Couchbase responses. Will return only the values
                as a dictionaries otherwise.

        Returns:
            :class:`couchbase.result.MultiGetResult` | List[Dict[Any, Any]] | None
        """
        args = {"keys": keys, "opts": self._build_opts("get", opts=opts)}

        try:
            ret = []
            self.session.cluster.wait_until_ready(
                timedelta(self.session.timeout.kv),
                WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
            )
            documents = self.session.collection.get_multi(**args).results
            for _, document in documents.items():
                ret.append(document if raw else document.content_as[dict])

            return ret
        except DocumentNotFoundException:
            return None

    def remove(self, key: str, opts: Optional[Dict[str, Any]] = None) -> bool:
        """Remove a single key

        Args:
            key (str):
                The key of the document to remove.
            opts (Dict[str, Any]):
                The operation options to use when removing the document.

        Returns:
            (bool):
                The status of the remove operation.
        """
        args = {"key": key, "opts": self._build_opts("remove", opts=opts)}

        self.session.cluster.wait_until_ready(
            timedelta(self.session.timeout.kv),
            WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
        )
        try:
            self.session.collection.remove(**args)
            return True
        except DocumentNotFoundException:
            pass

        return False

    def remove_multi(
        self, keys: List[str], opts: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Remove multiple keys

        Args:
            keys (List[str]):
                The keys of the documents to remove.
            opts (Dict[str, Any]):
                The operation options to use when removing the documents.

        Returns:
            (bool):
                The status of the remove operations.
        """
        args = {"keys": keys, "opts": self._build_opts("remove", opts=opts)}

        self.session.cluster.wait_until_ready(
            timedelta(self.session.timeout.kv),
            WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
        )
        try:
            result = self.session.collection.remove_multi(**args)

            if result.all_ok:
                return True

            for key, exception in result.exceptions.items():
                self.logger.error("unable to remove document %s: %s", key, exception)
        except Exception as _err:
            self.logger.error(
                "unhandled exception (%s): %s", type(_err).__name__, _err.args[0]
            )

        return False

    def delete(self, *args, **kwargs):
        """Alias for :class:`self.remove`"""
        return self.remove(*args, **kwargs)

    def delete_multi(self, *args, **kwargs):
        """Alias for :class:`self.remove_multi`"""
        return self.remove_multi(*args, **kwargs)

    def view_query(
        self,
        design_doc: str,
        view_name: str,
        *,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        opts: Optional[Dict[str, Any]] = None,
    ) -> Optional[List[Dict[Any, Any]]]:
        # TODO: method needs to be redone.
        if opts is None:
            opts = {}
        if limit is not None:
            opts["limit"] = limit
        if skip is not None:
            opts["skip"] = skip

        total_rows = 0
        try:
            self.session.cluster.wait_until_ready(
                timedelta(self.session.timeout.kv),
                WaitUntilReadyOptions(service_types=[ServiceType.View]),
            )
            query = self.session.bucket.view_query(
                design_doc=design_doc,
                view_name=view_name,
                **self._build_opts("view", opts=opts),
            )
            query_metadata = query.metadata()
            if query_metadata is not None:
                total_rows = query_metadata.total_rows()
            if query_metadata is None or (skip is not None and total_rows < skip):
                return query.rows()
        except Exception as _exc:
            self.logger.error(
                "CouchBase view query error (%s): %s",
                type(_exc).__name__,
                str(_exc.args[0]),
            )

        return None

    def n1ql(
        self,
        select: str = "*",
        where: Optional[Dict[str, Any]] = None,
        *,
        opts: Optional[Dict[str, Any]] = None,
    ) -> Optional[QueryResult]:
        # TODO: method needs to be redone.
        """
        generate and execute an N1QL query
        :param select: str
        the columns to select, defaults to "*" (all)
        :param where: Dict[str, Any]
        A key-value dictionary for the select statement
        :param opts: optional Dict[str, Any]
        any custom options to use for the query operation
        :return: List[Dict[Any, Any]] | None
        """
        if opts is None:
            opts = {}

        where_statement = ""
        if where is not None:
            for col, _ in where.items():
                if len(where_statement) > 0:
                    where_statement += " AND "
                else:
                    where_statement += " WHERE "
                where_statement += f"{col}=${col}"

        try:
            self.session.cluster.wait_until_ready(
                timedelta(self.session.timeout.kv),
                WaitUntilReadyOptions(service_types=[ServiceType.Query]),
            )
            query = N1QLQuery(
                f"SELECT {select} FROM `{self.session.bucket_name}`{where_statement}"
            )
            query.consistency = QueryScanConsistency.REQUEST_PLUS
            query.timeout = self.session.timeout.query
            return self.session.cluster.query(
                query.statement, **self._build_opts("query", opts=opts), **where
            ).rows()
        except Exception as _err:
            self.logger.error(
                "an error occurred when performing N1QL query (%s): %s",
                type(_err).__name__,
                _err.args[0],
            )

        return None

    def _build_opts(
        self,
        type_: str,
        *,
        opts: Optional[Dict[str, Any]] = None,
        expiry: Optional[Union[int, timedelta]] = None,
    ) -> Union[
        InsertOptions,
        InsertMultiOptions,
        UpsertOptions,
        UpsertMultiOptions,
        GetOptions,
        GetMultiOptions,
        QueryOptions,
        RemoveOptions,
        RemoveMultiOptions,
        ViewOptions,
    ]:
        """Generates operation options for specified operation type.

        Args:
            type_ (str):
                The type of operation to return options for.
            opts (Dict[str, Any]):
                Initial options to use for initiating the operation options instance.
            expiry (int | timedelta | None):
                Any general document expiry to use for the operations.

        Returns:
            Dict[str, Any]
        """
        if opts is None:
            opts = {}

        default_timeout = timedelta(seconds=self.session.timeout.kv)
        if type_ == "insert":
            base_options = InsertOptions
        elif type_ == "insert_multi":
            base_options = InsertMultiOptions
        elif type_ == "upsert":
            base_options = UpsertOptions
        elif type_ == "upsert_multi":
            base_options = UpsertMultiOptions
        elif type_ == "get":
            base_options = GetOptions
        elif type_ == "get_multi":
            base_options = GetMultiOptions
        elif type_ == "query":
            base_options = QueryOptions
            default_timeout = timedelta(seconds=self.session.timeout.query)
        elif type_ == "remove":
            base_options = RemoveOptions
        elif type_ == "remove_multi":
            base_options = RemoveMultiOptions
        elif type_ == "view":
            base_options = ViewOptions
        else:
            raise AttributeError(f"invalid attribute value {type_}")

        if "timeout" not in opts:
            opts["timeout"] = default_timeout

        ret = base_options(**opts)

        if ret is None:
            ret = {}

        if expiry is not None and isinstance(expiry, int):
            ret["expiry"] = timedelta(seconds=expiry)

        return ret
