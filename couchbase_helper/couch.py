from datetime import datetime, timedelta
import json
import logging
from typing import Any, Dict

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.diagnostics import ServiceType
from couchbase.exceptions import DocumentExistsException, DocumentNotFoundException
from couchbase.n1ql import N1QLQuery, QueryScanConsistency
from couchbase.options import (
    ClusterOptions,
    ClusterTimeoutOptions,
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
from couchbase.result import QueryResult


class CouchbaseHelper:
    def __init__(
        self,
        hostname: str,
        bucket: str,
        *,
        username: str | None = None,
        password: str,
        tls: bool = False,
        timeout: int = 10,
        wan: bool = False,
        dryrun: bool = False,
        output_folder: str = "output",
        logger: logging.Logger = None,
    ):
        if logger is None:
            logger = logging.getLogger()
        self.logger = logger

        self.logger.debug("Initiating couchbase connection:")
        # Create authenticator instance
        authenticator = PasswordAuthenticator(
            username=username or bucket,
            password=password,
        )

        # Create connection options
        self._timeout = timeout
        timeout_options = ClusterTimeoutOptions(
            connect_timeout=timedelta(seconds=self._timeout)
        )
        options = ClusterOptions(
            authenticator=authenticator,
            enable_tls=tls,
            timeout_options=timeout_options,
            enable_tracing=True,
            show_queries=True,
        )
        if wan:
            options.apply_profile("wan_development")

        # Initiate cluster and set bucket
        connection_string = f"couchbase{'s' if tls else ''}://{hostname}"
        self.logger.debug("- Connecting to cluster: %s", connection_string)
        self.cluster = Cluster(
            connection_string,
            options,
        )
        self.logger.debug("- Setting bucket: %s", bucket)
        self.cluster.wait_until_ready(timedelta(seconds=self._timeout))
        self.bucket = self.cluster.bucket(bucket)
        self.bucket_name = bucket

        # Set some operation options
        # ... maybe later?

        # Set default collection (we're dealing with a CB 6.6.0 server here)
        self.logger.debug("- Setting default collection")
        self.coll = self.bucket.default_collection()

        # Dry run?
        self._dryrun = dryrun
        if self._dryrun:
            self.logger.info("### RUNNING COUCHBASE CLASS IN DRY RUN MODE ###")
        self.output_folder = output_folder

    def insert(self, key: str, value, expiry=None, opts: dict = None):
        args = {
            "key": key,
            "value": value,
            "opts": self._build_opts("insert", opts=opts, expiry=expiry),
        }

        try:
            if not self._dryrun:
                self.cluster.wait_until_ready(
                    timedelta(self._timeout),
                    WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
                )
                return self.coll.insert(**args)
            else:
                self.logger.info("### DRYRUN: would insert key %s ###", key)
                self._save_dryrun_output(**args)
                return True
        except DocumentExistsException:
            return False

    def insert_multi(
            self, documents: dict, expiry=None, opts: dict = None, per_key_opts: dict = None
    ):
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
            if not self._dryrun:
                self.cluster.wait_until_ready(
                    timedelta(self._timeout),
                    WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
                )
                result = self.coll.insert_multi(**args)
                if result.all_ok:
                    return True

                for key, exception in result.exceptions.items():
                    self.logger.error("unable to add document %s: %s", key, exception)
            else:
                self.logger.info(
                    "### DRYRUN: would insert keys %s ###",
                    ", ".join(list(documents.keys())),
                )
                self._save_dryrun_outputs(**args)
        except Exception as _err:
            self.logger.error(
                "unhandled exception (%s): %s", type(_err).__name__, _err.args[0]
            )

        return False

    def upsert(self, key: str, value, expiry=None, opts: dict = None):
        args = {
            "key": key,
            "value": value,
            "opts": self._build_opts("insert", opts=opts, expiry=expiry),
        }

        try:
            if not self._dryrun:
                self.cluster.wait_until_ready(
                    timedelta(self._timeout),
                    WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
                )
                return self.coll.upsert(**args)
            else:
                self.logger.info("### DRYRUN: would upsert key %s ###", key)
                self._save_dryrun_output(**args)
                return True
        except DocumentNotFoundException:
            return False

    def upsert_multi(
        self, documents: dict, expiry=None, opts: dict = None, per_key_opts: dict = None
    ):
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
            if not self._dryrun:
                self.cluster.wait_until_ready(
                    timedelta(self._timeout),
                    WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
                )
                result = self.coll.upsert_multi(**args)
                if result.all_ok:
                    return True

                for key, exception in result.exceptions.items():
                    self.logger.error("unable to add document %s: %s", key, exception)
            else:
                self.logger.info(
                    "### DRYRUN: would upsert keys %s ###",
                    ", ".join(list(documents.keys())),
                )
                self._save_dryrun_outputs(**args)
        except Exception as _err:
            self.logger.error(
                "unhandled exception (%s): %s", type(_err).__name__, _err.args[0]
            )

        return False

    def get(self, key: str, opts: dict = None, *, raw: bool = False):
        args = {"key": key, "opts": self._build_opts("get", opts=opts)}

        try:
            self.cluster.wait_until_ready(
                timedelta(self._timeout),
                WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
            )
            document = self.coll.get(**args)
            return document if raw else document.content_as[dict]
        except DocumentNotFoundException:
            return None

    def get_multi(self, keys: list, opts: dict = None, *, raw: bool = False):
        args = {"keys": keys, "opts": self._build_opts("get", opts=opts)}

        try:
            ret = []
            self.cluster.wait_until_ready(
                timedelta(self._timeout),
                WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
            )
            documents = self.coll.get_multi(**args).results
            for _, document in documents.items():
                ret.append(document if raw else document.content_as[dict])

            return ret
        except DocumentNotFoundException:
            return None

    def remove(self, key: str, opts: dict = None):
        args = {"key": key, "opts": self._build_opts("remove", opts=opts)}

        if not self._dryrun:
            self.cluster.wait_until_ready(
                timedelta(self._timeout),
                WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
            )
            return self.coll.remove(**args)
        else:
            self.logger.info("### DRYRUN: would delete key %s ###", key)
            return True

    def remove_multi(self, keys: list, opts: dict = None):
        args = {"keys": keys, "opts": self._build_opts("remove", opts=opts)}

        if not self._dryrun:
            self.cluster.wait_until_ready(
                timedelta(self._timeout),
                WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]),
            )
            return self.coll.remove_multi(**args)
        else:
            self.logger.info("### DRYRUN: would delete keys %s ###", ", ".join(keys))
            return True

    def delete(self, *args, **kwargs):
        return self.remove(*args, **kwargs)

    def delete_multi(self, *args, **kwargs):
        return self.remove_multi(*args, **kwargs)

    def view_query(
        self,
        design_doc: str,
        view_name: str,
        *,
        limit: int | None = None,
        skip: int | None = None,
        opts: dict | None = None,
    ) -> list[dict] | None:
        if opts is None:
            opts = {}
        if limit is not None:
            opts["limit"] = limit
        if skip is not None:
            opts["skip"] = skip

        total_rows = 0
        try:
            self.cluster.wait_until_ready(
                timedelta(self._timeout),
                WaitUntilReadyOptions(service_types=[ServiceType.View]),
            )
            query = self.bucket.view_query(
                design_doc=design_doc,
                view_name=view_name,
                **self._build_opts("view", opts=opts),
            )
            query_metadata = query.metadata()
            if query_metadata is not None:
                total_rows = query_metadata.total_rows()
            if query_metadata is None or total_rows < skip:
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
        where: Dict[str, Any] | None = None,
        *,
        opts: Dict[str, Any] | None = None,
    ) -> QueryResult | None:
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
        for col, _ in where.items():
            if len(where_statement) > 0:
                where_statement += " AND "
            where_statement += f"{col}=${col}"

        try:
            self.cluster.wait_until_ready(
                timedelta(self._timeout),
                WaitUntilReadyOptions(service_types=[ServiceType.Query]),
            )
            query = N1QLQuery(
                f"SELECT {select} FROM `{self.bucket_name}` WHERE {where_statement}"
            )
            query.consistency = QueryScanConsistency.REQUEST_PLUS
            query.timeout = 2
            return self.cluster.query(
                query.statement, **self._build_opts("query", opts=opts), **where
            ).rows()
        except Exception as _err:
            self.logger.error(
                "an error occurred when performing N1QL query (%s): %s",
                type(_err).__name__,
                _err.args[0],
            )

        return None

    def _build_opts(self, type_: str, *, opts: dict = None, expiry: int = None) -> dict:
        """
        generate options object for specified action type
        :param type_: str
        can be one of 'insert', 'insert_multi', 'upsert', 'upsert_multi', 'get', 'get_multi', 'remove', 'remove_multi', and 'view'
        :param opts: dict
        a dict of applicable options for the operation being made
        :param expiry: int
        amount of seconds a document is valid (only for insert operations)
        :return: dict
        """
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
        elif type_ == "remove":
            base_options = RemoveOptions
        elif type_ == "remove_multi":
            base_options = RemoveMultiOptions
        elif type_ == "view":
            base_options = ViewOptions
        else:
            raise AttributeError(f"invalid attribute value {type_}")

        if opts is None:
            opts = {}

        if "timeout" not in opts:
            opts["timeout"] = timedelta(seconds=self._timeout)

        ret = base_options(**opts)

        if ret is None:
            ret = {}

        if expiry is not None and isinstance(expiry, int):
            ret["expiry"] = timedelta(seconds=expiry)

        return ret

    @staticmethod
    def datetime_handler(x):
        if isinstance(x, datetime):
            return x.isoformat()
        if isinstance(x, timedelta):
            return x.seconds
        raise TypeError("Unknown type")

    def _save_dryrun_output(self, key: str, value, opts: dict):
        """save a file locally instead of to couchbase as part of a dry run"""
        try:
            with open(f"{self.output_folder}/{key}.json", "w") as file:
                contents = {
                    "key": key,
                    "value": value,
                    "opts": opts,
                }
                file.write(
                    json.dumps(
                        contents,
                        default=self.datetime_handler,
                        indent=2,
                        sort_keys=True,
                    )
                )
        except Exception as _err:
            self.logger.error(
                "unable to save dry run file %s due to %s: %s",
                f"{self.output_folder}/{key}.json",
                type(_err).__name__,
                _err.args[0],
            )

    def _save_dryrun_outputs(self, keys_and_docs: dict, opts: dict):
        """save multiple files locally instead of to couchbase as part of a dry run"""
        for key, value in keys_and_docs.items():
            if "per_key_options" in opts and opts["per_key_options"] is not None:
                opts["per_key_options"] = opts["per_key_options"].get(key, None)
            self._save_dryrun_output(key, value, opts)
