import argparse

from collections.abc import Generator
from tclogger import logger, logstr, TCLogbar
from typing import Union

from .mongo_types import FilterIndexType, FilterOpType, FilterRangeType, SortOrderType
from .mongo_types import (
    MongoConfigsType,
    MongoCursorParamsType,
    MongoExtendParamsType,
    MongoCountParamsType,
    MongoFilterParamsType,
)
from .mongo import MongoOperator
from .mongo_filter import filters_str_to_mongo_filter
from .mongo_filter import extract_count_params_from_cursor_params


class MongoDocsGenerator:
    """Multi-step initializations:
    - `init_mongo`
    - `init_mongo_cursor`
    - `init_mongo_count`
    - `init_progress_bar`

    One-step initialization:
    - `init_cli_args`
    - `init_all_with_cli_args`
    """

    def __init__(self, verbose_mongo_args: bool = False):
        self.verbose_mongo_args = verbose_mongo_args

    def init_mongo(self, configs: MongoConfigsType):
        """Must calls this before using `self.mongo`. \n
        Use `cli_args_to_mongo_configs()` to pass params from CLI args.
        """
        self.configs = configs
        self.mongo = MongoOperator(
            configs=self.configs,
            connect_cls=self.__class__,
            verbose_args=self.verbose_mongo_args,
        )

    def init_mongo_cursor(
        self,
        collection: str = None,
        filter_index: FilterIndexType = None,
        filter_op: FilterOpType = None,
        filter_range: FilterRangeType = None,
        include_fields: list[str] = None,
        exclude_fields: list[str] = None,
        sort_index: FilterIndexType = None,
        sort_order: SortOrderType = None,
        skip_count: int = None,
        extra_filters: Union[dict, list[dict]] = None,
        no_cursor_timeout: bool = False,
        # non-cursor args
        max_count: int = None,
        estimate_count: bool = False,
        batch_size: int = 10000,
    ):
        """Must call this before using `self.cursor`. \n
        Use `cli_args_to_mongo_extend_params()` to pass params from CLI args.
        """
        self.collection = collection
        self.cursor_params: MongoCursorParamsType = {
            "collection": collection,
            "filter_index": filter_index,
            "filter_op": filter_op,
            "filter_range": filter_range,
            "include_fields": include_fields,
            "exclude_fields": exclude_fields,
            "sort_index": sort_index,
            "sort_order": sort_order,
            "skip_count": skip_count,
            "extra_filters": extra_filters,
            "no_cursor_timeout": no_cursor_timeout,
        }
        self.cursor = self.mongo.get_cursor(**self.cursor_params)
        # set non-cursor params
        self.max_count = max_count
        self.estimate_count = estimate_count
        self.batch_size = batch_size

    def init_mongo_count(self):
        """Must call this after `init_mongo_cursor()` and before using `self.total_count`."""
        self.count_params: MongoCountParamsType = (
            extract_count_params_from_cursor_params(self.cursor_params)
        )
        self.count_params["estimate_count"] = self.estimate_count
        self.total_count = self.mongo.get_total_count(**self.count_params)
        if self.total_count == 0:
            cursor_info_str = f"[{logstr.okay(self.configs['dbname'])}:{logstr.mesg(self.collection)}]"
            logger.warn(f"× No doc found in cursor: {cursor_info_str}")
            self.cursor = None
            return
        if self.max_count:
            self.total_count = min(self.total_count, self.max_count)

    def init_progress_bar(self):
        """Must call this before logging progress when iterating cursor."""
        self.doc_bar = TCLogbar(head=logstr.note("  * Doc:"))
        self.doc_bar.set_total(self.total_count)
        skip_count = self.cursor_params.get("skip_count")
        if skip_count:
            self.doc_bar.set_start_count(skip_count)
            self.doc_bar.set_count(skip_count)
        self.doc_bar.update(flush=True)

    def init_cli_args(self, ikvs: dict = None, jkvs: dict = None):
        """Must call this before using `self.args`. \n
        Modularizing this is to allow user to pass extra args via function.\n
        Priority: `ikvs` < CLI args < `jkvs`."""
        arg_parser = MongoDocsGeneratorArgParser()
        self.args: argparse.Namespace = arg_parser.args
        if ikvs:
            for k, v in ikvs.items():
                if getattr(self.args, k) is None:
                    setattr(self.args, k, v)
        if jkvs:
            for k, v in jkvs.items():
                setattr(self.args, k, v)

    def init_all_with_cli_args(
        self,
        set_mongo: bool = True,
        set_cursor: bool = True,
        set_count: bool = True,
        set_bar: bool = True,
    ):
        """Must call `init_cli_args()` before using this."""
        if set_mongo:
            mongo_configs = cli_args_to_mongo_configs(self.args)
            self.init_mongo(configs=mongo_configs)
        if set_cursor:
            extend_params = cli_args_to_mongo_extend_params(self.args)
            self.init_mongo_cursor(**extend_params)
        if set_count:
            self.init_mongo_count()
        if set_bar:
            self.init_progress_bar()

    def check_before_generate(self) -> bool:
        if not hasattr(self, "cursor") or self.cursor is None:
            func_str = logstr.mesg("init_mongo_cursor()")
            logger.err(f"× `self.cursor` not initialized: try `{func_str}`")
            return False
        if not hasattr(self, "total_count") or self.total_count is None:
            func_str = logstr.mesg("init_mongo_count()")
            logger.warn(f"* `self.total_count` not set: try `{func_str}`")
            self.total_count = None
            # does not matter, just warn and set to None
        if self.total_count == 0:
            logger.err(f"× No doc to generate")
            return False
        if not hasattr(self, "doc_bar") or self.doc_bar is None:
            func_str = logstr.mesg("init_progress_bar()")
            logger.warn(f"* `self.doc_bar` not initialized: try `{func_str}`")
            self.doc_bar = None
            # does not matter, just warn and set to None
        if not hasattr(self, "max_count"):
            logger.err(f"* `self.max_count` not set: use None")
            self.max_count = None
            # does not matter, just warn and set to None
        return True

    def is_exceed_max_count(self, idx: int) -> bool:
        if self.max_count and idx >= self.max_count:
            return True
        return False

    def doc_generator(self) -> Generator[dict, None, None]:
        """Before using this, must call: \n
        - `init_mongo`, `init_mongo_cursor()`, `init_mongo_count()`, `init_progress_bar()`
        """
        if not self.check_before_generate():
            return
        for idx, doc in enumerate(self.cursor):
            if self.is_exceed_max_count(idx):
                break
            yield doc
            if self.doc_bar:
                self.doc_bar.update(1)

    def update_progress_by_batch(self, docs_batch: list[dict]):
        if not self.doc_bar:
            return
        self.doc_bar.update(len(docs_batch), flush=True)

    def docs_batch_generator(self) -> Generator[list[dict], None, None]:
        """Before using this, must call: \n
        - `init_mongo`, `init_mongo_cursor()`, `init_mongo_count()`, `init_progress_bar()`
        """
        if not self.check_before_generate():
            return
        docs_batch = []
        for idx, doc in enumerate(self.cursor):
            if self.is_exceed_max_count(idx):
                break
            docs_batch.append(doc)
            if len(docs_batch) >= self.batch_size:
                yield docs_batch
                self.update_progress_by_batch(docs_batch)
                docs_batch = []
        if docs_batch:
            yield docs_batch
            self.update_progress_by_batch(docs_batch)
            docs_batch = []


class MongoDocsGeneratorArgParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super(MongoDocsGeneratorArgParser, self).__init__(*args, **kwargs)
        # config args: host, port, dbname
        self.add_argument("-H", "--host", type=str, default="localhost")
        self.add_argument("-P", "--port", type=int, default=27017)
        self.add_argument("-D", "--dbname", type=str)
        # cursor args: collection
        self.add_argument("-mc", "--mongo-collection", type=str)
        # cursor args: filters
        self.add_argument("-fi", "--filter-index", type=str, default=None)
        self.add_argument("-so", "--sort-order", type=str, default=None)
        self.add_argument("-rs", "--range-start", type=str, default=None)
        self.add_argument("-re", "--range-end", type=str, default=None)
        self.add_argument("-sc", "--skip-count", type=int, default=None)
        self.add_argument("-xf", "--extra-filters", type=str, default=None)
        self.add_argument("-nf", "--no-filter", action="store_true", default=False)
        # cursor args: in/ex-clude fields
        self.add_argument("-if", "--include-fields", type=str, default=None)
        self.add_argument("-ef", "--exclude-fields", type=str, default=None)
        # cursor args: max-count
        self.add_argument("-mn", "--max-count", type=int, default=None)
        # count args: estimate
        self.add_argument("-ec", "--estimate-count", action="store_true", default=False)
        # batch args: batch-size
        self.add_argument("-bs", "--batch-size", type=int, default=10000)
        # run args: dry-run
        self.add_argument("-dr", "--dry-run", action="store_true", default=False)
        self.args, _ = self.parse_known_args()


def cli_args_to_mongo_configs(args: argparse.Namespace) -> MongoConfigsType:
    mongo_configs: MongoConfigsType = {
        "host": args.host,
        "port": args.port,
        "dbname": args.dbname,
    }
    return mongo_configs


def cli_args_to_mongo_extend_params(args: argparse.Namespace) -> MongoExtendParamsType:
    common_params = {
        "collection": args.mongo_collection,
        "skip_count": args.skip_count,
        "no_cursor_timeout": False,
    }

    if args.no_filter:
        filter_params = {
            "filter_index": None,
            "filter_op": "range",
            "filter_range": [None, None],
        }
        sort_params = {
            "sort_index": None,
            "sort_order": None,
        }
    else:
        filter_params = {
            "filter_index": args.filter_index,
            "filter_op": "range",
            "filter_range": [args.range_start, args.range_end],
        }
        sort_params = {
            "sort_index": args.filter_index,
            "sort_order": args.sort_order,
        }

    if args.extra_filters:
        extra_filters = [filters_str_to_mongo_filter(args.extra_filters)]
    else:
        extra_filters = None
    filter_params["extra_filters"] = extra_filters

    if args.include_fields:
        include_fields = args.include_fields.split(",")
    else:
        include_fields = None
    if args.exclude_fields:
        exclude_fields = args.exclude_fields.split(",")
    else:
        exclude_fields = None
    fields_params = {
        "include_fields": include_fields,
        "exclude_fields": exclude_fields,
    }

    others_params = {
        "max_count": args.max_count,
        "estimate_count": args.estimate_count,
        "batch_size": args.batch_size,
    }

    extend_params = {
        **common_params,
        **filter_params,
        **sort_params,
        **fields_params,
        **others_params,
    }
    return extend_params
