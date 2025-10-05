from typing import Literal, TypedDict, Union


FilterIndexType = Literal["insert_at", "pubdate", "index_at"]
FilterOpType = Literal["gt", "lt", "gte", "lte", "range"]
FilterRangeType = Union[int, str, tuple, list]
SortOrderType = Literal["asc", "desc", "none"]

COUNT_ARG_KEYS = [
    "collection",
    "filter_index",
    "filter_op",
    "filter_range",
    "extra_filters",
    "estimate_count",
]
FILTER_ARG_KEYS = ["filter_index", "filter_op", "filter_range", "is_date_field"]
DATE_FIELDS = ["insert_at", "pubdate", "index_at"]


class MongoConfigsType(TypedDict):
    host: str
    port: int
    dbname: str


class MongoCursorParamsType(TypedDict):
    collection: str
    filter_index: str
    filter_op: FilterOpType
    filter_range: FilterRangeType
    include_fields: list[str]
    exclude_fields: list[str]
    sort_index: str
    sort_order: SortOrderType
    skip_count: int
    is_date_field: bool


class MongoCountParamsType(TypedDict):
    collection: str
    filter_index: str
    filter_op: FilterOpType
    filter_range: FilterRangeType
    estimate_count: bool


class MongoFilterParamsType(TypedDict):
    filter_index: str
    filter_op: FilterOpType
    filter_range: FilterRangeType
    is_date_field: bool
