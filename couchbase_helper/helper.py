from datetime import timedelta
import logging
from typing import Any, Dict, List, Optional, Union

from couchbase.diagnostics import ServiceType
from couchbase.exceptions import DocumentExistsException, DocumentNotFoundException
from couchbase.options import (
    InsertOptions,
    UpsertOptions,
    WaitUntilReadyOptions,
)
from couchbase.result import GetResult, MultiGetResult

from ._types import JSONType
from .options import build_opts
from .protocols import SessionProt


class CouchbaseHelper:
    """A couchbase helper class to simplify document operations

    Args:
        session (implements :class:`couchbase_helper.protocols.SessionProt`):
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
            value (`couchbase_helper._types.JSONType`):
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
            "opts": build_opts("insert", opts=opts, expiry=expiry),
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
                per_key_opts[key] = build_opts("insert_multi", opts=val)

            opts["per_key_options"] = per_key_opts

        args = {
            "keys_and_docs": documents,
            "opts": build_opts("insert_multi", opts=opts, expiry=expiry),
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
            value (`couchbase_helper._types.JSONType`):
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
            "opts": build_opts("upsert", opts=opts, expiry=expiry),
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
                per_key_opts[key] = build_opts("upsert", opts=val)

            opts["per_key_options"] = per_key_opts

        args = {
            "keys_and_docs": documents,
            "opts": build_opts("upsert_multi", opts=opts, expiry=expiry),
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

    def replace(
        self,
        key: str,
        value: JSONType,
        expiry: Optional[Union[int, timedelta]] = None,
        opts: Optional[Dict[str, Any]] = None,
    ):
        """Replace a single document.

        Args:
            key (str):
                The key of the document to save.
            value (`couchbase_helper._types.JSONType`):
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
            "opts": build_opts("replace", opts=opts, expiry=expiry),
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

    def replace_multi(
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
                per_key_opts[key] = build_opts("upsert", opts=val)

            opts["per_key_options"] = per_key_opts

        args = {
            "keys_and_docs": documents,
            "opts": build_opts("replace_multi", opts=opts, expiry=expiry),
        }
        try:
            self.session.cluster.wait_until_ready(
                timedelta(self.session.timeout.kv),
                WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
            )
            result = self.session.collection.replace_multi(**args)
            if result.all_ok:
                return True

            for key, exception in result.exceptions.items():
                self.logger.error("unable to replace document %s: %s", key, exception)
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
        args = {"key": key, "opts": build_opts("get", opts=opts)}

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
        args = {"keys": keys, "opts": build_opts("get_multi", opts=opts)}

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
        args = {"key": key, "opts": build_opts("remove", opts=opts)}

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
        args = {"keys": keys, "opts": build_opts("remove_multi", opts=opts)}

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
        """Alias for :class:`~.remove`"""
        return self.remove(*args, **kwargs)

    def delete_multi(self, *args, **kwargs):
        """Alias for :class:`~.remove_multi`"""
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
                **build_opts("view", opts=opts),
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
